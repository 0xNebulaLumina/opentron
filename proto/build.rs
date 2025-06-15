fn main() {
    const PROTOS: &[&str] = &[
        "proto/api/api.proto",
        "proto/api/zksnark.proto",
        "proto/core/contract/account_contract.proto",
        "proto/core/contract/asset_issue_contract.proto",
        "proto/core/contract/balance_contract.proto",
        "proto/core/contract/common.proto",
        "proto/core/contract/exchange_contract.proto",
        "proto/core/contract/market_contract.proto",
        "proto/core/contract/proposal_contract.proto",
        "proto/core/contract/shield_contract.proto",
        "proto/core/contract/smart_contract.proto",
        "proto/core/contract/storage_contract.proto",
        "proto/core/contract/vote_asset_contract.proto",
        "proto/core/contract/witness_contract.proto",
        "proto/core/tron/account.proto",
        "proto/core/tron/block.proto",
        "proto/core/tron/delegated_resource.proto",
        "proto/core/tron/p2p.proto",
        "proto/core/tron/proposal.proto",
        "proto/core/tron/transaction.proto",
        "proto/core/tron/vote.proto",
        "proto/core/tron/witness.proto",
        "proto/core/Discover.proto",
        "proto/core/Tron.proto",
        "proto/core/TronInventoryItems.proto",
        "proto/google/api/annotations.proto",
        "proto/google/api/http.proto",
    ];

    for f in PROTOS {
        println!("cargo:rerun-if-changed={}", f);
    }
    prost_build::Config::new()
        .type_attribute("proto.common.SmartContract.ABI", "#[derive(serde::Serialize)]")
        .type_attribute("proto.common.SmartContract.ABI.Entry", "#[derive(serde::Serialize)]")
        .type_attribute("proto.common.SmartContract.ABI.Param", "#[derive(serde::Serialize)]")
        .out_dir("src")
        .compile_protos(PROTOS, &["proto/"])
        .unwrap();
}
