# muse-bridge plugin v2

Claude Code 플러그인: home의 Claude Code가 Muse VM에서 도는 서브에이전트에게
작업을 위임하는 브릿지.

## 구성

```
muse-bridge/
  .claude-plugin/plugin.json   플러그인 매니페스트
  .mcp.json                    MCP 서버 선언 (muse-bridge)
  servers/bridge/bridge_mcp.py MCP 서버 (stdio)
  skills/muse-delegate/        위임 방법 스킬 (자동 로드)
  commands/muse.md             /muse-bridge:muse — 위임+폴링
  commands/muse-status.md      /muse-bridge:status — 상태 조회
```

## MCP 도구

- `muse_submit(prompt, label="", workers=1, worker_instructions="", priority=0, timeout_minutes=60)`
  → task_id. `workers` 1-8, `worker_instructions`는 단일 문자열(전체에 방송)
  또는 JSON 배열(워커별 지시).
- `muse_result(task_id)` → done(마크다운 결과)/running/pending/cancelled
- `muse_status()` → 워커 풀 heartbeat, 대기/실행 중 작업
- `muse_cancel(task_id)` → 대기 작업 즉시 취소, 실행 중 작업은 취소 요청

## 브릿지 파일 규약 (home `C:\Users\ghfud\muse-bridge\`)

- `queue.json` — MCP 서버만 기록 (submit/cancel)
- `claims/<id>.json` — 마스터 claim `{id, assignment:0, worker, workers_effective, claimed_at, instructions}`
- `claims/<id>.a<j>.json` — 슬롯 claim (멀티워커 작업)
- `results/<id>.md` — 최종 결과 / `results/<id>.part-<j>.md` — 워커별 중간 산출물
- `status/worker-<n>.json` — 워커 heartbeat `{worker, state, task_id, assignment, updated_at}`

## 설치 (home PC)

```powershell
claude plugin marketplace add C:\Users\ghfud\muse-marketplace
claude plugin install muse-bridge@muse-marketplace
claude plugin list   # muse-bridge 확인
```

## Muse 측

- saved workflow `muse-bridge-worker` — 상주 워커 풀 (기본 2개, args.pool_size로 조정)
- 각 워커는 `~/workspace/muse-bridge/worker/bw.py` 로 큐 기계적 처리 + 에이전트 본체가 작업 수행
- 멀티워커 작업: 마스터 claim → 유휴 워커가 슬롯 claim → 각자 part 작성 → 마스터가 합성
- keeper cron `muse-bridge-keeper` (6시간 간격) — 워커 사망 시 재실행
