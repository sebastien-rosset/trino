# GPUs

| Rank | Category | Vendor | GPU Model | Cost (USD) | Memory | Memory Bandwidth | AI Performance (Peak) | FP32 Performance | Power | Performance/Watt (TFLOPS FP8 or FP16 per Watt) | Architecture | Primary Use Case |
|------|----------|--------|-----------|------------|--------|------------------|----------------------|------------------|-------|-----------------------------------------------|--------------|---------------------|
| 1 | Datacenter | NVIDIA | GB200 Superchip | $60,000-70,000 | 384GB HBM3E (2x192GB) | 16TB/s | ~20 PFLOPS (FP4) | N/A | 2,400W | **~8,333 TFLOPS/W (FP4)** | Blackwell | Extreme-scale AI training, frontier models |
| 2 | Datacenter | NVIDIA | B200 | $35,000-40,000 | 192GB HBM3E | 8TB/s | ~4 PFLOPS (FP8) | N/A | 1,000W | **~4,000 TFLOPS/W (FP8)** | Blackwell | Large-scale AI training & inference |
| 3 | Datacenter | NVIDIA | H200 | $35,000-40,000 | 141GB HBM3e | 4.8TB/s | 4 PFLOPS (FP8) | N/A | 700W | **~5,714 TFLOPS/W (FP8)** | Hopper | LLM training & inference, HPC |
| 4 | Datacenter | AMD | MI355X | Not disclosed | 288GB HBM3E | 8TB/s | Est. ~2 PFLOPS (FP8) | N/A | 1,400W peak | **~1,429 TFLOPS/W (FP8)** | CDNA 4 (3nm) | Extreme AI workloads, H200 competitor |
| 5 | Datacenter | NVIDIA | H100 | $30,000-35,000 | 80GB HBM3 | 3.35TB/s | 2 PFLOPS (FP16) | N/A | 700W (SXM) / 400W (PCIe) | **~2,857 TFLOPS/W (FP16 SXM)** / **~5,000 (PCIe)** | Hopper | AI training, HPC, widely deployed |
| 6 | Datacenter | AMD | MI325X | Not disclosed | 256GB HBM3E | 6TB/s | 2.6 PFLOPS (FP8) | N/A | 750W | **~3,467 TFLOPS/W (FP8)** | CDNA 3 | Large AI models, H200 alternative |
| 7 | Datacenter | AMD | MI300X | $25,000-30,000 est. | 192GB HBM3 | 5.3TB/s | 1.3 PFLOPS (mixed) | N/A | 750W | **~1,733 TFLOPS/W (mixed)** | CDNA 3 | AI inference/training, cost-effective |
| 8 | Datacenter | NVIDIA | A100 80GB | $10,000-15,000 | 80GB HBM2e | 2.0TB/s | 624 TFLOPS (FP16 w/ sparsity) | 19.5 TFLOPS | 400W (SXM) / 300W (PCIe) | **~1,560 TFLOPS/W (FP16 SXM)** / **~2,080 (PCIe)** | Ampere | General AI/HPC, MIG support, mature |
| 9 | Enterprise | NVIDIA | RTX PRO 6000 | $8,600-11,000 | 96GB GDDR7 | ~1TB/s est. | ~2 PFLOPS (FP8 est.) | 125 TFLOPS | 600W (WS/Server) / 300W (Max-Q) | **~3,333 TFLOPS/W (FP8 Server)** / **~417 FP32/W** | Blackwell | Enterprise AI + graphics hybrid |
| 10 | Enterprise | NVIDIA | L40S | $7,500-10,000 | 48GB GDDR6 | 864GB/s | 1.466 PFLOPS (FP8) | 91.6 TFLOPS | 350W | **~4,189 TFLOPS/W (FP8)** / **~262 FP32/W** | Ada Lovelace | AI inference + graphics, cost-effective |
| 11 | Enterprise | NVIDIA | L40 | $7,000-8,000 | 48GB GDDR6 | 864GB/s | ~1.3 PFLOPS (FP8 est.) | 91.6 TFLOPS | 300W | **~4,333 TFLOPS/W (FP8)** / **~305 FP32/W** | Ada Lovelace | Universal datacenter, AI + graphics |
| 12 | Datacenter | Intel | Gaudi 3 | $15,625 (~$125K for 8-GPU) | 128GB HBM2E | 3.67TB/s | 1.835 PFLOPS (BF16/FP8) | N/A | 600W | **~3,058 TFLOPS/W (BF16)** | Custom | AI inference, Ethernet networking |
| 13 | Enterprise | AMD | Radeon Pro W7900 | $3,999 | 48GB GDDR6 ECC | 864GB/s | N/A | 61 TFLOPS | 295W | **~207 FP32/W** | RDNA 3 | Professional 3D, VFX, CAD, rendering |
| 14 | Enterprise | NVIDIA | A40 | $5,000-7,000 | 48GB GDDR6 | 696GB/s | 150 TFLOPS (FP16) | 37.4 TFLOPS | 300W | **~500 TFLOPS/W (FP16)** / **~125 FP32/W** | Ampere | Virtual workstations, mixed AI + graphics |
| 15 | Enterprise | NVIDIA | A30 | $4,000-6,000 | 24GB HBM2 | 933GB/s | 330 TFLOPS (FP16) | 10.3 TFLOPS | 165W | **~2,000 TFLOPS/W (FP16)** / **~62 FP32/W** | Ampere | Mainstream AI, MIG, multi-tenancy |
| 16 | Edge | NVIDIA | L4 | $3,000-5,000 | 24GB GDDR6 | 300GB/s | ~242 TFLOPS (FP8) | 30.3 TFLOPS | 72W | **🏆 ~3,361 TFLOPS/W (FP8)** / **~421 FP32/W 🏆** | Ada Lovelace | Edge inference, video analytics, efficiency |
| 17 | Enterprise | NVIDIA | A10 | $2,500-3,500 | 24GB GDDR6 | 600GB/s | 125 TFLOPS (FP16) | 31.2 TFLOPS | 150W | **~833 TFLOPS/W (FP16)** / **~208 FP32/W** | Ampere | Virtual desktops, graphics workstations |
| 18 | Enterprise | AMD | Radeon Pro W7800 | $2,499 | 32GB GDDR6 ECC | 576GB/s | N/A | 45 TFLOPS | 260W | **~173 FP32/W** | RDNA 3 | Content creation, engineering, CAD |
| 19 | Edge | NVIDIA | T4 | $2,000-3,000 | 16GB GDDR6 | 320GB/s | 65 TFLOPS (Tensor) | 8.1 TFLOPS | 70W | **~929 TFLOPS/W (Tensor)** / **~116 FP32/W** | Turing (legacy) | Legacy inference, VDI, cost-sensitive |