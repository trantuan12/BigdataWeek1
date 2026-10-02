$ErrorActionPreference = "Stop"
$NS = "bd-g01"
$STORAGE_IMAGE = "chris-seaweedfs:latest"  # Placeholder
$CLIENT_IMAGE = "s3client:v1"              # Placeholder

Write-Host "1. Sinh S3 Identities..."
python make_identities.py

Write-Host "2. Tạo Kubernetes Secrets..."
kubectl -n $NS create secret generic s3-config --from-file=s3.json=private/s3.json --dry-run=client -o yaml | kubectl apply -f -
foreach ($role in @("owner", "ingestor", "analyst")) {
    kubectl -n $NS create secret generic "s3-$role" --from-env-file="private/$role.env" --dry-run=client -o yaml | kubectl apply -f -
}

Write-Host "3. Triển khai store.yaml..."
$storeYaml = Get-Content store.yaml -Raw
$storeYaml = $storeYaml.Replace('${STORAGE_IMAGE}', $STORAGE_IMAGE)
$storeYaml | kubectl -n $NS apply -f -

Write-Host "4. Triển khai clients.yaml..."
$clientsYaml = Get-Content clients.yaml -Raw
$clientsYaml = $clientsYaml.Replace('${CLIENT_IMAGE}', $CLIENT_IMAGE)
$clientsYaml | kubectl -n $NS apply -f -

Write-Host "5. Chờ Deployment objects và Pods sẵn sàng..."
kubectl -n $NS rollout status deployment/objects --timeout=120s
kubectl -n $NS wait --for=condition=Ready pod/owner pod/ingestor pod/analyst pod/blocked --timeout=120s

Write-Host "6. Lưu thông tin topology..."
New-Item -ItemType Directory -Force -Path evidence | Out-Null
kubectl -n $NS get pod,pvc,svc,endpointslice -o wide > evidence/topology.txt

Write-Host "7. Chạy lệnh seed dữ liệu (Fixtures)..."
kubectl -n $NS exec owner -- python /opt/s3lab.py seed > evidence/seed.jsonl

Write-Host "8. Kiểm tra đọc dữ liệu với Ingestor và Analyst..."
kubectl -n $NS exec ingestor -- python /opt/s3lab.py probe get research-raw fixture.txt
kubectl -n $NS exec analyst -- python /opt/s3lab.py probe get research-release fixture.txt

Write-Host "Task B Deploy hoàn tất!"
