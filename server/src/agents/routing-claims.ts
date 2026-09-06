/**
 * The claim row behind a `one-of-us` election (#70).
 *
 * Wake delivery is already single-owner per message (`claimAndWake` SETNXes
 * `cumora:wake-claim:<messageId>` before `wake()` runs), so this row is NOT a
 * mutual-exclusion primitive. It is the durable memory of the election:
 *
 *   - observable — an operator can see which agent was elected for which
 *     message and how many candidates were burnt before one turned;
 *   - leaseable — the elected primary has LEASE_MS to start a turn; a sweeper
 *     advances to the next ordered candidate when the lease lapses with no
 *     `agent_runs` row from the primary, so a no-show degrades to the next
 *     agent instead of a silent room;
 *   - recoverable — if the wake-claim TTL lapses and the message re-delivers,
 *     the existing row is honored (same primary, same cursor) instead of
 *     re-electing.
 *
 * Cursor semantics: `candidates[cursor]` is the agent currently holding the
 * wake. `candidates` is stored in the deterministic order produced by
 * `orderCandidates` — the sweep walks it, it never re-orders.
 *
 * Multi-replica safety of the sweep: a due row is taken with
 * `UPDATE … WHERE lease_expires_at < NOW() RETURNING`, so exactly one replica
 * advances each round; the renewed lease keeps the others out until then.
 */
import { pool } from '../db/pool.js'
import { env } from '../env.js'
import type { ElectionCandidate } from './routing-election.js'

/** How long the elected primary has to START a turn before the sweep moves on.
 *  Generous on purpose: a cloud pod may be cold-booting, a BYOA daemon may be
 *  mid-reconnect; both deliver the turn from the durable inbox once alive. */
export const ELECTION_LEASE_MS = 90_000
const SWEEP_INTERVAL_MS = 15_000
const SWEEP_BATCH = 20
/** Terminal rows are operator-facing history, not state — reap after a day. */
const TERMINAL_ROW_TTL_MS = 24 * 60 * 60_000

export type ClaimStatus = 'pending' | 'served' | 'exhausted'

export interface RoutingClaim {
  messageId: string
  companyId: string | null
  conversationId: string
  candidates: string[]
  cursor: number
  status: ClaimStatus
  cursorAdvancedAt?: Date | null
}

/** Read the wake candidates' roster rows (role + busy-status lease) for the
 *  election. Candidates that departed mid-flight are dropped — they must not
 *  be electable even if membership still lists them. */
export async function loadElectionCandidates(agentIds: readonly string[]): Promise<ElectionCandidate[]> {
  if (agentIds.length === 0) return []
  const { rows } = await pool.query<{
    id: string; role: string | null; status: string | null; statusUpdatedAt: Date | null
  }>(
    `SELECT id, role, status, status_updated_at AS "statusUpdatedAt"
       FROM participants
      WHERE id = ANY($1::text[])
        AND kind = 'agent'
        AND departed_at IS NULL`,
    [[...agentIds]],
  )
  return rows
}

/** Write the election's winner. Returns the row when THIS caller won the
 *  insert; when a row already exists (the message re-delivered after the wake
 *  claim TTL), returns the existing row so the caller honors it instead of
 *  re-electing. */
export async function claimPrimary(args: {
  messageId: string
  companyId: string | null
  conversationId: string
  orderedCandidates: readonly string[]
  leaseMs?: number
}): Promise<RoutingClaim | null> {
  const leaseMs = args.leaseMs ?? ELECTION_LEASE_MS
  const { rows } = await pool.query<RoutingClaimRow>(
    `INSERT INTO agent_routing_claims
         (message_id, company_id, conversation_id, candidates, cursor, cursor_advanced_at, status, lease_expires_at)
       VALUES ($1, $2, $3, $4::text[], 0, NOW(), 'pending', NOW() + ($5::int * INTERVAL '1 millisecond'))
       ON CONFLICT (message_id) DO NOTHING
       RETURNING message_id AS "messageId", company_id AS "companyId", conversation_id AS "conversationId",
                 candidates, cursor, status, cursor_advanced_at AS "cursorAdvancedAt"`,
    [args.messageId, args.companyId, args.conversationId, [...args.orderedCandidates], leaseMs],
  )
  if (rows[0]) return toClaim(rows[0])
  return await getClaim(args.messageId)
}

export async function getClaim(messageId: string): Promise<RoutingClaim | null> {
  const { rows } = await pool.query<RoutingClaimRow>(
    `SELECT message_id AS "messageId", company_id AS "companyId", conversation_id AS "conversationId",
            candidates, cursor, status, cursor_advanced_at AS "cursorAdvancedAt"
       FROM agent_routing_claims
      WHERE message_id = $1`,
    [messageId],
  )
  return rows[0] ? toClaim(rows[0]) : null
}

interface RoutingClaimRow {
  messageId: string
  companyId: string | null
  conversationId: string
  candidates: string[]
  cursor: number
  status: ClaimStatus
  cursorAdvancedAt?: Date | null
}

function toClaim(r: RoutingClaimRow): RoutingClaim {
  return {
    messageId: r.messageId,
    companyId: r.companyId,
    conversationId: r.conversationId,
    candidates: [...r.candidates],
    cursor: r.cursor,
    status: r.status,
    cursorAdvancedAt: r.cursorAdvancedAt,
  }
}

/** One sweep pass. Returns the wakes the sweep decided on, if any — the caller
 *  (the interval worker) performs them, so this module stays free of the
 *  scheduler's import cycle.
 *
 *  `advance` moves the wake to the next candidate. `exhaust` is the terminal
 *  state: every candidate held the wake and started no turn, so the room is
 *  handed back to the pre-election behaviour — a full fan-out of the original
 *  lineup — rather than being left to each member's next natural wake. Wakes
 *  are durable in the inbox, so for members already back online this is
 *  redundant; for the ones whose laptop was shut during their window it is
 *  the difference between the message being processed and being skipped past
 *  by a later read-cursor ack (see the #70 discussion on cursor semantics). */
export type SweepDecision =
  | { kind: 'advance'; agentId: string; conversationId: string }
  | { kind: 'exhaust'; conversationId: string; room: string[] }
  | { kind: 'catchup'; conversationId: string; room: string[] }

export async function sweepRoutingClaimsOnce(opts: {
  leaseMs?: number
  hasRunSince?: (agentId: string, since: Date, companyId: string | null) => Promise<boolean>
  findLaggingMembers?: (conversationId: string, messageId: string, companyId: string | null, excludeAgentId: string) => Promise<string[]>
} = {}): Promise<SweepDecision[]> {
  const leaseMs = opts.leaseMs ?? ELECTION_LEASE_MS
  const hasRunSince = opts.hasRunSince ?? defaultHasRunSince
  const findLaggingMembers = opts.findLaggingMembers ?? defaultFindCursorLaggingMembers
  const decisions: SweepDecision[] = []

  const due = await pool.query<TakenRow>(
    `WITH due AS (
       SELECT message_id
         FROM agent_routing_claims
        WHERE status = 'pending'
          AND lease_expires_at < NOW()
        ORDER BY lease_expires_at ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
     )
     UPDATE agent_routing_claims c
        SET lease_expires_at = NOW() + ($1::int * INTERVAL '1 millisecond'),
            updated_at = NOW()
       FROM due
      WHERE c.message_id = due.message_id
     RETURNING c.message_id AS "messageId", c.company_id AS "companyId", c.conversation_id AS "conversationId",
               c.candidates, c.cursor, c.status, c.created_at AS "createdAt",
               c.cursor_advanced_at AS "cursorAdvancedAt"`,
    [leaseMs, SWEEP_BATCH],
  )

  for (const row of due.rows) {
    const primary = row.candidates[row.cursor]
    if (!primary) {
      // Defensive: an out-of-range cursor can only come from manual edits.
      await pool.query(`UPDATE agent_routing_claims SET status = 'exhausted', updated_at = NOW() WHERE message_id = $1`, [row.messageId])
      continue
    }
    const anchor = row.cursorAdvancedAt ?? row.createdAt
    if (await hasRunSince(primary, anchor, row.companyId)) {
      await pool.query(`UPDATE agent_routing_claims SET status = 'served', updated_at = NOW() WHERE message_id = $1`, [row.messageId])
      const lagging = await findLaggingMembers(row.conversationId, row.messageId, row.companyId, primary).catch((err) => {
        console.error(`[routing] findCursorLaggingMembers failed for message ${row.messageId}:`, err instanceof Error ? err.message : err)
        return []
      })
      if (lagging.length > 0) {
        console.log(`[routing] claim for message ${row.messageId} served by ${primary} — waking ${lagging.length} cursor-lagging room member(s) to close exposure window`)
        decisions.push({ kind: 'catchup', conversationId: row.conversationId, room: lagging })
      }
      continue
    }
    const nextCursor = row.cursor + 1
    if (nextCursor >= row.candidates.length) {
      await pool.query(`UPDATE agent_routing_claims SET status = 'exhausted', updated_at = NOW() WHERE message_id = $1`, [row.messageId])
      console.warn(`[routing] election for message ${row.messageId}: every candidate went quiet — falling back to the full-room fan-out`)
      decisions.push({ kind: 'exhaust', conversationId: row.conversationId, room: [...row.candidates] })
      continue
    }
    await pool.query(
      `UPDATE agent_routing_claims
          SET cursor = $2,
              cursor_advanced_at = NOW(),
              lease_expires_at = NOW() + ($3::int * INTERVAL '1 millisecond'),
              updated_at = NOW()
        WHERE message_id = $1 AND cursor = $4`,
      [row.messageId, nextCursor, leaseMs, row.cursor],
    )
    console.warn(`[routing] elected primary ${primary} for message ${row.messageId} produced no turn within its lease — advancing to ${row.candidates[nextCursor]}`)
    decisions.push({ kind: 'advance', agentId: row.candidates[nextCursor], conversationId: row.conversationId })
  }

  // Reap terminal rows so the table stays operator-sized.
  await pool.query(
    `DELETE FROM agent_routing_claims
      WHERE status <> 'pending'
        AND updated_at < NOW() - ($1::int * INTERVAL '1 millisecond')`,
    [TERMINAL_ROW_TTL_MS],
  )
  return decisions
}

interface TakenRow extends RoutingClaimRow {
  createdAt: Date
}

/** Did the agent start ANY turn since the claim? Coarse on purpose: a run in
 *  another room proves the agent is alive and turning, and wrongly "serving"
 *  a claim costs one silent room — the same silence the pre-election world
 *  lived in. Wrongly advancing on a live agent would double-wake. */
async function defaultHasRunSince(agentId: string, since: Date, companyId: string | null): Promise<boolean> {
  const { rows } = await pool.query<{ id: string }>(
    `SELECT id FROM agent_runs
      WHERE agent_id = $1
        AND started_at > $2
        AND ($3::text IS NULL OR company_id = $3)
      LIMIT 1`,
    [agentId, since, companyId],
  )
  return rows.length > 0
}

/** Find any active agent member in the conversation whose read cursor
 *  is still behind this message. Used on claim resolution ('served') to
 *  close the cursor-exposure window: room members that were not woken
 *  for the election are woken once the claim resolves, catching their
 *  cursor up and preventing silent message skipping on a subsequent post. */
export async function defaultFindCursorLaggingMembers(
  conversationId: string,
  messageId: string,
  companyId: string | null,
  excludeAgentId: string,
): Promise<string[]> {
  const { rows } = await pool.query<{ participant_id: string }>(
    `SELECT cm.participant_id
       FROM conversation_members cm
       JOIN participants p
         ON p.id = cm.participant_id
        AND p.company_id = cm.company_id
        AND p.kind = 'agent'
        AND p.departed_at IS NULL
       JOIN messages m
         ON m.id = $1
       LEFT JOIN conversation_reads cr
         ON cr.user_id = cm.participant_id
        AND cr.conversation_id = cm.conversation_id
      WHERE cm.conversation_id = $2
        AND ($3::text IS NULL OR cm.company_id = $3)
        AND cm.participant_id <> $4
        AND ROW(COALESCE(cr.last_read_at, '1970-01-01T00:00:00Z'::timestamptz), COALESCE(cr.last_read_message_id, ''))
            < ROW(m.created_at, m.id)
      ORDER BY cm.ordinal ASC`,
    [messageId, conversationId, companyId, excludeAgentId],
  )
  return rows.map((r) => r.participant_id)
}

function startRoutingClaimSweeper(intervalMs: number = SWEEP_INTERVAL_MS): NodeJS.Timeout {
  const tick = (): void => {
    sweepRoutingClaimsOnce()
      .then((decisions) => {
        for (const d of decisions) {
          void (async () => {
            const scheduler = await import('./scheduler.js')
            if (d.kind === 'advance') {
              await scheduler.wakeAgent(d.agentId, 'message.new', d.conversationId)
            } else {
              // Both 'exhaust' and 'catchup' fan out to the candidate list:
              // - exhaust: election expired without a run; fall back to pre-election full fan-out
              // - catchup: claim resolved (served); wake cursor-lagging members so their read cursor
              //   catches up, eliminating the cursor-exposure window.
              await scheduler.fanOutWake(d.room, d.conversationId, null)
            }
          })().catch((err) => {
            console.error(`[routing] lease-advance wake failed (${d.kind}):`, err instanceof Error ? err.message : err)
          })
        }
      })
      .catch((err) => console.error('[routing] election sweep failed:', err instanceof Error ? err.message : err))
  }
  setImmediate(tick)
  const t = setInterval(tick, intervalMs)
  t.unref?.()
  return t
}

/** Started by the scheduler module once the server is up. No-op when the
 *  one-of-us route is disabled — no elections are written, so nothing to sweep. */
export function startRoutingClaimSweeperIfEnabled(): NodeJS.Timeout | null {
  if (!env.ROUTING_ONE_OF_US) return null
  return startRoutingClaimSweeper()
}
