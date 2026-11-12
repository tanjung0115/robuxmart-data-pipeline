"""
generate_source_data.py
-----------------------
Generates realistic RobuxMart OLTP source datasets based on the exact schema
from RobuxMart (`C:\\fix devsec\\backend\\database\\migrations`), enriched with:
- Realistic multi-week transaction volume (~600 transactions)
- Semi-structured JSON event logs (`interaction_logs`, ~1,200 events)
- Intentional real-world Data Quality anomalies (webhook duplicates, inconsistent
  payment method strings, missing roblox_id, negative/zero price glitches)
  to demonstrate ETL cleaning, deduplication, and Dead-Letter Quarantine.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# Exact packages from RobuxMart's PackageSeeder.php
ROBUXMART_PACKAGES: List[Dict[str, Any]] = [
    {
        "id": 1,
        "name": "Paket Starter",
        "robux_amount": 400,
        "price": 50000.00,
        "description": "Paket starter untuk pemula",
        "image": "starter.png",
        "is_active": True,
        "created_at": "2026-10-01 08:00:00",
        "updated_at": "2026-10-01 08:00:00",
    },
    {
        "id": 2,
        "name": "Paket Basic",
        "robux_amount": 800,
        "price": 95000.00,
        "description": "Paket basic dengan harga terjangkau",
        "image": "basic.png",
        "is_active": True,
        "created_at": "2026-10-01 08:00:00",
        "updated_at": "2026-10-01 08:00:00",
    },
    {
        "id": 3,
        "name": "Paket Popular",
        "robux_amount": 1700,
        "price": 185000.00,
        "description": "Paket paling populer",
        "image": "popular.png",
        "is_active": True,
        "created_at": "2026-10-01 08:00:00",
        "updated_at": "2026-10-01 08:00:00",
    },
    {
        "id": 4,
        "name": "Paket Premium",
        "robux_amount": 4500,
        "price": 475000.00,
        "description": "Paket premium dengan bonus Robux",
        "image": "premium.png",
        "is_active": True,
        "created_at": "2026-10-01 08:00:00",
        "updated_at": "2026-10-01 08:00:00",
    },
    {
        "id": 5,
        "name": "Paket Ultimate",
        "robux_amount": 10000,
        "price": 950000.00,
        "description": "Paket ultimate untuk kebutuhan besar",
        "image": "ultimate.png",
        "is_active": True,
        "created_at": "2026-10-01 08:00:00",
        "updated_at": "2026-10-01 08:00:00",
    },
    {
        "id": 6,
        "name": "Paket VIP",
        "robux_amount": 22500,
        "price": 1950000.00,
        "description": "Paket VIP eksklusif",
        "image": "vip.png",
        "is_active": True,
        "created_at": "2026-10-01 08:00:00",
        "updated_at": "2026-10-01 08:00:00",
    },
]

INDONESIAN_NAMES = [
    ("John Doe", "johndoe", "customer@robuxmart.com"),
    ("Jane Smith", "janesmith", "jane@example.com"),
    ("Ahmad Santoso", "ahmads", "ahmad@example.com"),
    ("Budi Pratama", "budipratama", "budi.pratama@gmail.com"),
    ("Siti Nurhaliza", "sitinur", "siti.nur@yahoo.com"),
    ("Reza Rahadian", "rezar", "reza.gamer@gmail.com"),
    ("Dewi Lestari", "dewil", "dewi.lestari@outlook.com"),
    ("Fajar Nugroho", "fajarnugroho", "fajar.nugroho@gmail.com"),
    ("Putri Ayu", "putriayu", "putri.ayu@gmail.com"),
    ("Rizky Febian", "rizkyf", "rizky.febian@gmail.com"),
    ("Nadia Vega", "nadiav", "nadia.vega@gmail.com"),
    ("Dimas Anggara", "dimasa", "dimas.anggara@gmail.com"),
    ("Arief Muhammad", "ariefm", "arief.muh@gmail.com"),
    ("Tasyi Athasyia", "tasyia", "tasyi.ath@gmail.com"),
    ("Kevin Sanjaya", "kevins", "kevin.sanjaya@gmail.com"),
    ("Windah Basudara", "brando_windah", "windah.bocil@gmail.com"),
    ("Luthfi Halimawan", "luthfih", "luthfi.halim@gmail.com"),
    ("MiawAug Reggy", "miawaug", "reggy.miaw@gmail.com"),
    ("citra kirana", "citrak", "citra.kirana@yahoo.com"),
    ("Gilang Dirga", "gilangd", "gilang.dirga@gmail.com"),
    ("Raffi Ahmad", "raffia", "raffi.rans@gmail.com"),
    ("Nagita Slavina", "nagitas", "nagita.rans@gmail.com"),
    ("Deddy Corbuzier", "deddyc", "deddy.pod@gmail.com"),
    ("Jerome Polin", "jeromep", "jerome.mantappu@gmail.com"),
    ("Jess No Limit", "jessnolimit", "tobias.justin@gmail.com"),
    ("Sisca Kohl", "siscak", "sisca.mari@gmail.com"),
    ("Frost Diamond", "frostd", "kananda.widyantara@gmail.com"),
    ("BeaconCream", "beaconcream", "nelson.wijaya@gmail.com"),
    ("Erpan1140", "erpan1140", "andi.irfan@gmail.com"),
    ("zenmatho", "zenmatho", "zaki.matho@gmail.com"),
]

PAYMENT_METHODS_CLEAN = ["QRIS", "GoPay", "OVO", "DANA", "BCA_VA", "MANDIRI_VA", "SHOPEEPAY"]
PAYMENT_METHODS_MESSY = [" qris ", "gopay", "Gopay ", "dana", "bca_va", " ovo", "QRIS"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Safari/17.2",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) Chrome/120.0.6099.144 Mobile",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def generate_all_source_files(seed: int = 42) -> Dict[str, int]:
    """Generate raw CSV and JSON extracts simulating RobuxMart OLTP tables."""
    random.seed(seed)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    base_date = datetime(2026, 10, 14, 9, 0, 0)

    # 1. Generate Users (Admin + 30 Customers)
    users: List[Dict[str, Any]] = [
        {
            "id": 1,
            "name": "Admin RobuxMart",
            "username": "admin_robuxmart",
            "email": "admin@admin.com",
            "password": "$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi",
            "phone": "081100000001",
            "avatar": "admin.png",
            "is_admin": True,
            "email_verified_at": "2026-10-01 08:00:00",
            "created_at": "2026-10-01 08:00:00",
            "updated_at": "2026-10-01 08:00:00",
        }
    ]

    for idx, (full_name, uname, email) in enumerate(INDONESIAN_NAMES, start=2):
        reg_time = base_date + timedelta(days=random.randint(0, 10), hours=random.randint(0, 20))
        verified = reg_time + timedelta(minutes=random.randint(2, 60)) if random.random() < 0.85 else ""
        users.append(
            {
                "id": idx,
                "name": full_name,
                "username": uname,
                "email": email,
                "password": f"$2y$10$hashedpasswordsim{idx:04d}xyz",
                "phone": f"0812{random.randint(10000000, 99999999)}",
                "avatar": f"avatar_{idx}.png",
                "is_admin": False,
                "email_verified_at": verified.strftime("%Y-%m-%d %H:%M:%S") if verified else "",
                "created_at": reg_time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": reg_time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # 2. Generate Transactions (600 valid + intentional messy/duplicate/corrupted records)
    transactions: List[Dict[str, Any]] = []
    customer_ids = [u["id"] for u in users if not u["is_admin"]]
    package_weights = [0.25, 0.25, 0.24, 0.14, 0.08, 0.04]

    for tx_id in range(1, 601):
        uid = random.choice(customer_ids)
        pkg = random.choices(ROBUXMART_PACKAGES, weights=package_weights, k=1)[0]
        day_offset = random.randint(0, 28)
        tx_time = base_date + timedelta(
            days=day_offset,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        # Occasional messy payment_method formatting (to test normalization)
        if random.random() < 0.08:
            pay_method = random.choice(PAYMENT_METHODS_MESSY)
        else:
            pay_method = random.choice(PAYMENT_METHODS_CLEAN)

        status_roll = random.random()
        if status_roll < 0.78:
            status = "success"
        elif status_roll < 0.91:
            status = "failed"
        else:
            status = "pending"

        transactions.append(
            {
                "id": tx_id,
                "user_id": uid,
                "package_id": pkg["id"],
                "package_name": pkg["name"],
                "robux_amount": pkg["robux_amount"],
                "price": pkg["price"],
                "roblox_id": str(10000000 + uid * 37),
                "roblox_username": f"rblx_{users[uid - 1]['username']}",
                "payment_method": pay_method,
                "payment_token": f"TOK-{tx_id:05d}-{random.randint(1000, 9999)}",
                "status": status,
                "notes": "Auto-fulfilled via RobuxMart API" if status == "success" else "Gateway callback",
                "created_at": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": (tx_time + timedelta(seconds=45)).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # Inject 8 Exact/Webhook Duplicate rows (same transaction ID resent by payment gateway)
    for dup_source in transactions[:8]:
        dup_copy = dict(dup_source)
        dup_copy["notes"] = "Webhook duplicate retry"
        transactions.append(dup_copy)

    # Inject 6 Corrupted/Invalid rows that MUST be caught and quarantined by ETL
    corrupted_rows = [
        {
            "id": 9001,
            "user_id": 2,
            "package_id": 1,
            "package_name": "Paket Starter",
            "robux_amount": 400,
            "price": -50000.00,  # Invalid negative price
            "roblox_id": "10000074",
            "roblox_username": "rblx_johndoe",
            "payment_method": "QRIS",
            "payment_token": "TOK-ERR-9001",
            "status": "success",
            "notes": "Glitch negative price",
            "created_at": "2026-11-05 14:10:00",
            "updated_at": "2026-11-05 14:10:05",
        },
        {
            "id": 9002,
            "user_id": 3,
            "package_id": 2,
            "package_name": "Paket Basic",
            "robux_amount": 800,
            "price": 0.00,  # Invalid zero price
            "roblox_id": "10000111",
            "roblox_username": "rblx_janesmith",
            "payment_method": "GoPay",
            "payment_token": "TOK-ERR-9002",
            "status": "success",
            "notes": "Zero price exploit attempt",
            "created_at": "2026-11-06 11:20:00",
            "updated_at": "2026-11-06 11:20:05",
        },
        {
            "id": 9003,
            "user_id": 4,
            "package_id": 3,
            "package_name": "Paket Popular",
            "robux_amount": 1700,
            "price": 185000.00,
            "roblox_id": "",  # Missing mandatory destination roblox_id
            "roblox_username": "",
            "payment_method": "OVO",
            "payment_token": "TOK-ERR-9003",
            "status": "pending",
            "notes": "Missing roblox_id",
            "created_at": "2026-11-07 16:45:00",
            "updated_at": "2026-11-07 16:45:05",
        },
        {
            "id": 9004,
            "user_id": 9999,  # Non-existent foreign key user_id
            "package_id": 1,
            "package_name": "Paket Starter",
            "robux_amount": 400,
            "price": 50000.00,
            "roblox_id": "10099999",
            "roblox_username": "ghost_user",
            "payment_method": "DANA",
            "payment_token": "TOK-ERR-9004",
            "status": "failed",
            "notes": "Orphaned user_id",
            "created_at": "2026-11-08 19:00:00",
            "updated_at": "2026-11-08 19:00:10",
        },
        {
            "id": 9005,
            "user_id": 5,
            "package_id": 4,
            "package_name": "Paket Premium",
            "robux_amount": -4500,  # Negative robux amount
            "price": 475000.00,
            "roblox_id": "10000185",
            "roblox_username": "rblx_budipratama",
            "payment_method": "BCA_VA",
            "payment_token": "TOK-ERR-9005",
            "status": "failed",
            "notes": "Invalid robux_amount",
            "created_at": "2026-11-09 09:30:00",
            "updated_at": "2026-11-09 09:30:05",
        },
        {
            "id": 9006,
            "user_id": 6,
            "package_id": 2,
            "package_name": "Paket Basic",
            "robux_amount": 800,
            "price": 95000.00,
            "roblox_id": "10000222",
            "roblox_username": "rblx_sitinur",
            "payment_method": "QRIS",
            "payment_token": "TOK-ERR-9006",
            "status": "REFUNDED_UNKNOWN",  # Invalid enum status outside pending/success/failed
            "notes": "Unsupported status enum",
            "created_at": "2026-11-10 21:15:00",
            "updated_at": "2026-11-10 21:15:12",
        },
    ]
    transactions.extend(corrupted_rows)

    # 3. Generate Interaction Logs (~1,000 events including JSON details & brute-force anomaly)
    interaction_logs: List[Dict[str, Any]] = []
    actions = ["PAGE_VIEW", "LOGIN_SUCCESS", "TRANSACTION_CREATE", "LOGIN_FAILED", "ERROR_JS"]
    action_weights = [0.50, 0.22, 0.18, 0.06, 0.04]

    for log_id in range(1, 1001):
        act = random.choices(actions, weights=action_weights, k=1)[0]
        event_time = base_date + timedelta(
            days=random.randint(0, 28),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        ua = random.choice(USER_AGENTS)

        if act == "LOGIN_FAILED":
            uid = random.choice(customer_ids + [None])
            ip = "192.168.1.100" if random.random() < 0.65 else f"103.10.{random.randint(1, 99)}.{random.randint(1, 250)}"
            details = {"email": "attacker@unknown.org", "reason": "Invalid credentials"}
            page_url = "http://localhost:8080/login.html"
        elif act == "ERROR_JS":
            uid = None
            ip = f"114.122.{random.randint(1, 99)}.{random.randint(1, 250)}"
            details = {"error": "Uncaught TypeError: Cannot read properties of null", "stack": "at checkout.js:84"}
            page_url = "http://localhost:8080/form.html"
        elif act == "TRANSACTION_CREATE":
            uid = random.choice(customer_ids)
            pkg = random.choice(ROBUXMART_PACKAGES)
            ip = f"36.72.{random.randint(1, 99)}.{random.randint(1, 250)}"
            details = {"package": pkg["name"], "robux": pkg["robux_amount"], "amount": pkg["price"]}
            page_url = "http://localhost:8080/form.html"
        elif act == "LOGIN_SUCCESS":
            uid = random.choice(customer_ids)
            ip = f"36.72.{random.randint(1, 99)}.{random.randint(1, 250)}"
            details = {"email": users[uid - 1]["email"], "ip": ip}
            page_url = "http://localhost:8080/login.html"
        else:
            uid = random.choice(customer_ids + [None])
            ip = f"180.241.{random.randint(1, 99)}.{random.randint(1, 250)}"
            details = {"page": random.choice(["Home", "Catalog", "Checkout", "Dashboard"]), "duration_sec": random.randint(5, 180)}
            page_url = "http://localhost:8080/index.html"

        interaction_logs.append(
            {
                "id": log_id,
                "user_id": uid,
                "action": act,
                "details": json.dumps(details),
                "ip_address": ip,
                "user_agent": ua,
                "page_url": page_url,
                "created_at": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    # Write CSVs and JSON
    _write_csv(RAW_DIR / "users.csv", users)
    _write_csv(RAW_DIR / "packages.csv", ROBUXMART_PACKAGES)
    _write_csv(RAW_DIR / "transactions.csv", transactions)
    with open(RAW_DIR / "interaction_logs.json", "w", encoding="utf-8") as f:
        json.dump(interaction_logs, f, indent=2)

    return {
        "users": len(users),
        "packages": len(ROBUXMART_PACKAGES),
        "transactions": len(transactions),
        "interaction_logs": len(interaction_logs),
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    counts = generate_all_source_files()
    print(f"Generated raw RobuxMart datasets in {RAW_DIR}: {counts}")
