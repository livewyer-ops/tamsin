# Exit codes

| Code | Meaning |
| ---: | --- |
| `0` | Every input succeeded, resumed, or completed its dry run |
| `1` | Internal or output failure |
| `2` | Invalid arguments or configuration |
| `3` | Authentication failed or credentials are unsafe for the transport |
| `4` | A batch completed with at least one failed input |
| `5` | Input discovery or reading failed |
| `6` | FFprobe or FFmpeg failed, is missing, or is unsupported |
| `7` | TAMS preflight, mutation, transfer, or verification failed |
| `8` | The run was interrupted or its parent context ended |

For `--format json`, a complete run repeats the code in
`run.finished.exit_code`. Input and diagnostic events carry stable failure
codes such as `config.invalid`, `source.failed`, `media.failed`,
`tams.preflight_failed`, and `object.stranded`.

The process code classifies the run; inspect terminal events for the affected
input, Flow and Object UUIDs. In particular, exit `4` can include both
successful and failed inputs. EOF without `run.finished` is incomplete and may
not have a trustworthy exit record.

SIGINT and SIGTERM request graceful cancellation. TAMSin stops scheduling new
work, completes one terminal event for each declared input where possible, and
then exits `8`. A hard kill can prevent terminal output and cleanup.
