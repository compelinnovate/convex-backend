use common::types::ModuleEnvironment;
use serde::{
    Deserialize,
    Serialize,
};
use sync_types::{
    CanonicalizedModulePath,
    CanonicalizedUdfPath,
    ModulePath,
};
use value::{
    codegen_convex_serialization,
    sha256::Sha256Digest,
    DeveloperDocumentId,
};

use super::module_versions::{
    AnalyzedModule,
    SerializedAnalyzedModule,
};
use crate::{
    config::types::{
        format_module_environment,
        parse_persisted_module_environment_and_pool,
        NodeExecutorPoolName,
    },
    modules::module_versions::AnalyzedFunction,
    source_packages::types::SourcePackageId,
};

/// In-memory representation of a module's metadata.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModuleMetadata {
    /// Path stored as a "path" field.
    pub path: CanonicalizedModulePath,

    /// The source package ID of the module. For any active source_package_id it
    /// is safe to read from that package or any subsequent package because we
    /// will update the reference if there is a diff
    pub source_package_id: SourcePackageId,
    pub environment: ModuleEnvironment,
    pub node_pool: Option<NodeExecutorPoolName>,
    pub analyze_result: Option<AnalyzedModule>,
    // This is a hash of source + source_map.
    pub sha256: Sha256Digest,
}

impl ModuleMetadata {
    // Returns true if the module's contents match the other module's contents. The
    // source_package_id may have changed, so we ignore it.
    pub fn matches_module_contents(&self, other: &ModuleMetadata) -> bool {
        self.path == other.path
            && self.analyze_result == other.analyze_result
            && self.environment == other.environment
            && self.node_pool == other.node_pool
            && self.sha256 == other.sha256
    }

    pub fn find_analyzed_function(
        &self,
        udf_path: &CanonicalizedUdfPath,
    ) -> anyhow::Result<Option<AnalyzedFunction>> {
        // Dependency modules don't have AnalyzedModule.
        if !udf_path.module().is_deps() {
            let analyzed_module = self
                .analyze_result
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Expected analyze result for {udf_path:?}"))?;

            for function in &analyzed_module.functions {
                if &function.name == udf_path.function_name() {
                    return Ok(Some(function.clone()));
                }
            }
        }

        Ok(None)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedModuleMetadata {
    pub path: String,
    pub source_package_id: String,
    pub environment: String,
    pub node_pool: Option<String>,
    pub analyze_result: Option<SerializedAnalyzedModule>,
    pub sha256: String,
}

impl TryFrom<SerializedModuleMetadata> for ModuleMetadata {
    type Error = anyhow::Error;

    fn try_from(m: SerializedModuleMetadata) -> anyhow::Result<Self> {
        let (environment, node_pool) =
            parse_persisted_module_environment_and_pool(&m.environment, m.node_pool)?;
        Ok(Self {
            path: {
                let path: ModulePath = m.path.parse()?;
                path.assume_canonicalized()?
            },
            source_package_id: DeveloperDocumentId::decode(&m.source_package_id)?.into(),
            environment,
            node_pool,
            analyze_result: m.analyze_result.map(|s| s.try_into()).transpose()?,
            sha256: Sha256Digest::from_base64(&m.sha256)?,
        })
    }
}

impl TryFrom<ModuleMetadata> for SerializedModuleMetadata {
    type Error = anyhow::Error;

    fn try_from(m: ModuleMetadata) -> anyhow::Result<Self> {
        Ok(Self {
            path: String::from(m.path),
            source_package_id: DeveloperDocumentId::from(m.source_package_id).to_string(),
            environment: format_module_environment(m.environment, m.node_pool.as_ref()),
            node_pool: m.node_pool.map(|name| name.to_string()),
            analyze_result: m.analyze_result.map(|s| s.try_into()).transpose()?,
            sha256: m.sha256.as_base64(),
        })
    }
}

codegen_convex_serialization!(ModuleMetadata, SerializedModuleMetadata);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_node_pool_on_non_node_metadata() {
        let metadata = SerializedModuleMetadata {
            path: "consumer.js".to_owned(),
            source_package_id: DeveloperDocumentId::MIN.encode(),
            environment: ModuleEnvironment::Isolate.to_string(),
            node_pool: Some("consumer".to_owned()),
            analyze_result: None,
            sha256: Sha256Digest::from([0; 32]).as_base64(),
        };
        assert!(ModuleMetadata::try_from(metadata).is_err());
    }

    #[test]
    fn durable_node_pool_environment_marker_round_trips_and_reads_legacy_records() {
        let legacy = SerializedModuleMetadata {
            path: "consumer.js".to_owned(),
            source_package_id: DeveloperDocumentId::MIN.encode(),
            environment: ModuleEnvironment::Node.to_string(),
            node_pool: Some("consumer".to_owned()),
            analyze_result: None,
            sha256: Sha256Digest::from([0; 32]).as_base64(),
        };
        let metadata = ModuleMetadata::try_from(legacy).unwrap();
        let serialized = SerializedModuleMetadata::try_from(metadata).unwrap();
        assert_eq!(serialized.environment, "node:pool:consumer");
        assert_eq!(serialized.node_pool.as_deref(), Some("consumer"));
    }
}
