# Chỉ dẫn minh chứng năm task

| Task | Kết quả và minh chứng |
|---|---|
| 1 Guardrails | quota-before.yaml, quota-after.yaml, quota-reject.txt, resource-budget.txt; [events của quota-positive](run-20261002T162540Z/K02.stdout) chứng minh Pod nhỏ đã chạy |
| 2 Storage | topology.txt, seed.jsonl, provenance.json; [raw fixture read](run-20261002T162540Z/S02.stdout), [release fixture read](run-20261002T162540Z/S06.stdout); snapshots Pod/PVC/image trong task5 bổ sung trạng thái của lần recovery |
| 3 Governance | [security-results.csv](../security-results.csv), [chỉ dẫn Task 3](task3/README.md); run-20261002T162540Z và task3 chứa request/control outputs |
| 4 Benchmark | r1-c1.jsonl, r1-c4.jsonl, r2-c4.jsonl, r2-c1.jsonl, r3-c1.jsonl, r3-c4.jsonl; resource-samples.txt; [bảng kết quả](../benchmark-summary.csv) |
| 5 Recovery | [lần chạy recovery](task5/run-20261003T030554169124Z/): Pod/PVC/Service trước và sau, verify-before/after, canary, delete-time, policy retests và load checks; [tổng hợp](../recovery-summary.json) |

versions.yaml ghi phiên bản môi trường. Các file topology và provenance có Pod/PVC UID khác nhau vì được thu thập từ nhiều lần triển khai; không ghép thành một snapshot duy nhất. Raw logs được giữ để người đánh giá đối chiếu kết quả.

Hồ sơ chưa có ảnh chụp minh họa riêng và logs/events sau recovery được xuất riêng. Báo cáo cá nhân hiện có Role C và D. Các mục chưa hoàn tất được ghi trong báo cáo nhóm và bảng kết quả.
