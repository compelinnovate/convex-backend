use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fmt::Formatter,
    ops::Deref,
    str::FromStr,
};

use common::{
    knobs::MAX_ZIPPED_PACKAGES_SIZE,
    types::ObjectKey,
};
use errors::ErrorMetadata;
use humansize::{
    FormatSize,
    BINARY,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_bytes::ByteBuf;
use sync_types::CanonicalizedModulePath;
use value::{
    codegen_convex_serialization,
    heap_size::HeapSize,
    id_v6::DeveloperDocumentId,
    sha256::Sha256Digest,
};

use crate::{
    config::types::NodeExecutorPoolName,
    external_packages::types::ExternalDepsPackageId,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeExecutorPoolTopology {
    assignments: BTreeMap<CanonicalizedModulePath, NodeExecutorPoolName>,
    default_route_count: Option<usize>,
    default_routes: Option<BTreeSet<CanonicalizedModulePath>>,
}

impl NodeExecutorPoolTopology {
    /// Construct topology read from a record that predates exact default-route
    /// membership. New topology should use [`Self::new_complete`].
    pub fn new(
        assignments: BTreeMap<CanonicalizedModulePath, NodeExecutorPoolName>,
        default_route_count: Option<usize>,
    ) -> Self {
        Self {
            assignments,
            default_route_count,
            default_routes: None,
        }
    }

    pub fn new_complete(
        assignments: BTreeMap<CanonicalizedModulePath, NodeExecutorPoolName>,
        default_routes: BTreeSet<CanonicalizedModulePath>,
    ) -> Self {
        Self {
            assignments,
            default_route_count: Some(default_routes.len()),
            default_routes: Some(default_routes),
        }
    }

    pub fn default_route_count(&self) -> Option<usize> {
        self.default_route_count
    }

    pub fn default_routes(&self) -> Option<&BTreeSet<CanonicalizedModulePath>> {
        self.default_routes.as_ref()
    }

    pub fn matches_archive(&self, archive: &Self) -> bool {
        self.assignments == archive.assignments
            && match &self.default_routes {
                Some(routes) => archive.default_routes.as_ref() == Some(routes),
                None => self
                    .default_route_count
                    .is_none_or(|count| archive.default_route_count == Some(count)),
            }
    }
}

impl Deref for NodeExecutorPoolTopology {
    type Target = BTreeMap<CanonicalizedModulePath, NodeExecutorPoolName>;

    fn deref(&self) -> &Self::Target {
        &self.assignments
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NodeVersion {
    /// Node 18 is deprecated in AWS, so customers with it set will
    /// no longer be able to update their static lambdas. This is okay because
    /// users can update their Node version to unblock themselves. This also
    /// means that new deployments with Node 18 will fail.
    V18x,
    V20x,
    V22x,
    V24x,
}

impl FromStr for NodeVersion {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "18" => Ok(NodeVersion::V18x),
            "20" => Ok(NodeVersion::V20x),
            "22" => Ok(NodeVersion::V22x),
            "24" => Ok(NodeVersion::V24x),
            _ => anyhow::bail!("Invalid node version: {value}"),
        }
    }
}

impl From<NodeVersion> for String {
    fn from(value: NodeVersion) -> String {
        match value {
            NodeVersion::V18x => "18".to_string(),
            NodeVersion::V20x => "20".to_string(),
            NodeVersion::V22x => "22".to_string(),
            NodeVersion::V24x => "24".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeVersionDiff {
    pub previous_version: Option<NodeVersion>,
    pub next_version: Option<NodeVersion>,
}

#[derive(Serialize, Deserialize)]
pub struct SerializedNodeVersionDiff {
    pub previous_version: Option<String>,
    pub next_version: Option<String>,
}

impl TryFrom<NodeVersionDiff> for SerializedNodeVersionDiff {
    type Error = anyhow::Error;

    fn try_from(value: NodeVersionDiff) -> anyhow::Result<Self> {
        Ok(SerializedNodeVersionDiff {
            previous_version: value.previous_version.map(String::from),
            next_version: value.next_version.map(String::from),
        })
    }
}

impl TryFrom<SerializedNodeVersionDiff> for NodeVersionDiff {
    type Error = anyhow::Error;

    fn try_from(value: SerializedNodeVersionDiff) -> anyhow::Result<Self> {
        Ok(NodeVersionDiff {
            previous_version: value.previous_version.map(|s| s.parse()).transpose()?,
            next_version: value.next_version.map(|s| s.parse()).transpose()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Contains the metadata for a source package. Multiple [`SourcePackage`]
/// documents may be referenced in the modules table. [`ModuleMetadata`] that
/// reference old versions of [`SourcePackage`] are able to be read at all
/// subsequent versions of [`SourcePackage`].
pub struct SourcePackage {
    pub storage_key: ObjectKey,
    pub sha256: Sha256Digest,
    pub external_deps_package_id: Option<ExternalDepsPackageId>,
    pub package_size: PackageSize,
    pub node_version: Option<NodeVersion>,
    /// Complete module-to-pool topology for this source package.
    pub node_executor_pool_topology: NodeExecutorPoolTopology,
}

impl SourcePackage {
    pub fn metadata_matches(&self, other: &SourcePackage) -> bool {
        // We explicitly do not compare the sha256 because it also includes creation
        // time in the hash
        self.node_version == other.node_version
            && self.external_deps_package_id == other.external_deps_package_id
            && self.node_executor_pool_topology == other.node_executor_pool_topology
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct PackageSize {
    pub zipped_size_bytes: usize,
    pub unzipped_size_bytes: usize,
}

impl std::ops::Add for PackageSize {
    type Output = PackageSize;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            zipped_size_bytes: self.zipped_size_bytes + rhs.zipped_size_bytes,
            unzipped_size_bytes: self.unzipped_size_bytes + rhs.unzipped_size_bytes,
        }
    }
}

impl std::ops::AddAssign for PackageSize {
    fn add_assign(&mut self, rhs: Self) {
        self.zipped_size_bytes += rhs.zipped_size_bytes;
        self.unzipped_size_bytes += rhs.unzipped_size_bytes;
    }
}

impl std::fmt::Display for PackageSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(Zipped: {}, Unzipped, {})",
            self.zipped_size_bytes, self.unzipped_size_bytes
        )
    }
}

const MAX_UNZIPPED_PACKAGES_SIZE: usize = 230_000_000; // 230 MB - Lambda gives us 250 MB

impl PackageSize {
    pub fn verify_size(&self) -> anyhow::Result<()> {
        if self.zipped_size_bytes >= *MAX_ZIPPED_PACKAGES_SIZE {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ModulesTooLarge",
                format!(
                    "Total module size exceeded the zipped maximum ({} > maximum size {})",
                    self.zipped_size_bytes.format_size(BINARY),
                    MAX_ZIPPED_PACKAGES_SIZE.format_size(BINARY)
                ),
            ),);
        }
        if self.unzipped_size_bytes >= MAX_UNZIPPED_PACKAGES_SIZE {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ModulesTooLarge",
                format!(
                    "Total module size exceeded the unzipped maximum ({} > maximum size {})",
                    self.unzipped_size_bytes.format_size(BINARY),
                    MAX_UNZIPPED_PACKAGES_SIZE.format_size(BINARY)
                ),
            ),);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedPackageSize {
    zipped_size_bytes: i64,
    unzipped_size_bytes: i64,
}

impl TryFrom<SerializedPackageSize> for PackageSize {
    type Error = anyhow::Error;

    fn try_from(value: SerializedPackageSize) -> Result<Self, Self::Error> {
        let zipped_size_bytes: usize = value.zipped_size_bytes.try_into()?;
        let unzipped_size_bytes: usize = value.unzipped_size_bytes.try_into()?;
        Ok(PackageSize {
            zipped_size_bytes,
            unzipped_size_bytes,
        })
    }
}

impl TryFrom<PackageSize> for SerializedPackageSize {
    type Error = anyhow::Error;

    fn try_from(value: PackageSize) -> Result<Self, Self::Error> {
        Ok(SerializedPackageSize {
            zipped_size_bytes: value.zipped_size_bytes.try_into()?,
            unzipped_size_bytes: value.unzipped_size_bytes.try_into()?,
        })
    }
}

codegen_convex_serialization!(PackageSize, SerializedPackageSize);

#[derive(Debug, Clone, PartialEq, Eq, Copy, PartialOrd, Ord, Hash)]
pub struct SourcePackageId(DeveloperDocumentId);

impl HeapSize for SourcePackageId {
    fn heap_size(&self) -> usize {
        self.0.heap_size()
    }
}

impl From<DeveloperDocumentId> for SourcePackageId {
    fn from(id: DeveloperDocumentId) -> Self {
        Self(id)
    }
}

impl From<SourcePackageId> for String {
    fn from(value: SourcePackageId) -> Self {
        let id: DeveloperDocumentId = value.into();
        id.into()
    }
}
impl TryFrom<String> for SourcePackageId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let id = DeveloperDocumentId::decode(&value)?;
        Ok(SourcePackageId(id))
    }
}

impl From<SourcePackageId> for DeveloperDocumentId {
    fn from(id: SourcePackageId) -> DeveloperDocumentId {
        id.0
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedSourcePackage {
    storage_key: String,
    sha256: ByteBuf,
    external_package_id: Option<String>,
    package_size: Option<SerializedPackageSize>,
    node_version: Option<String>,
    node_pool_module_paths: Option<Vec<String>>,
    node_pool_names: Option<Vec<String>>,
    node_pool_default_route_count: Option<i64>,
    node_pool_default_module_paths: Option<Vec<String>>,
}

impl TryFrom<SourcePackage> for SerializedSourcePackage {
    type Error = anyhow::Error;

    fn try_from(value: SourcePackage) -> anyhow::Result<Self> {
        let node_pool_default_route_count = value
            .node_executor_pool_topology
            .default_route_count
            .map(i64::try_from)
            .transpose()?;
        let node_pool_default_module_paths = value
            .node_executor_pool_topology
            .default_routes
            .map(|routes| routes.into_iter().map(String::from).collect());
        let (node_pool_module_paths, node_pool_names) = value
            .node_executor_pool_topology
            .assignments
            .into_iter()
            .map(|(path, pool)| (String::from(path), pool.to_string()))
            .unzip();
        Ok(SerializedSourcePackage {
            storage_key: value.storage_key.into(),
            sha256: ByteBuf::from(value.sha256.to_vec()),
            external_package_id: value
                .external_deps_package_id
                .map(|id| DeveloperDocumentId::from(id).encode()),
            package_size: Some(value.package_size.try_into()?),
            node_version: value.node_version.map(String::from),
            node_pool_module_paths: Some(node_pool_module_paths),
            node_pool_names: Some(node_pool_names),
            node_pool_default_route_count,
            node_pool_default_module_paths,
        })
    }
}
impl TryFrom<SerializedSourcePackage> for SourcePackage {
    type Error = anyhow::Error;

    fn try_from(value: SerializedSourcePackage) -> Result<Self, Self::Error> {
        let storage_key = value.storage_key.try_into()?;
        let sha256 = value.sha256.into_vec().try_into()?;
        let external_package_id = match value.external_package_id {
            None => None,
            Some(s) => Some(DeveloperDocumentId::decode(&s)?.into()),
        };
        let package_size: PackageSize = match value.package_size {
            Some(o) => o.try_into()?,
            // Just use default for old source packages
            None => PackageSize::default(),
        };
        let node_version = match value.node_version {
            None => None,
            Some(s) => Some(s.parse()?),
        };
        let (assignments, default_route_count, default_routes) = match (
            value.node_pool_module_paths,
            value.node_pool_names,
            value.node_pool_default_route_count,
            value.node_pool_default_module_paths,
        ) {
            (None, None, None, None) => (BTreeMap::new(), None, None),
            (Some(paths), Some(names), default_route_count, default_route_paths) => {
                anyhow::ensure!(
                    paths.len() == names.len(),
                    "Source package Node pool topology fields have different lengths"
                );
                let mut topology = BTreeMap::new();
                for (path, name) in paths.into_iter().zip(names) {
                    anyhow::ensure!(
                        topology.insert(path.parse()?, name.parse()?).is_none(),
                        "Source package Node pool topology contains a duplicate module"
                    );
                }
                let default_route_count = default_route_count.map(usize::try_from).transpose()?;
                let default_routes = default_route_paths
                    .map(|paths| {
                        let mut routes = BTreeSet::new();
                        for path in paths {
                            let path = path.parse()?;
                            anyhow::ensure!(
                                routes.insert(path),
                                "Source package default Node topology contains a duplicate module"
                            );
                        }
                        anyhow::Ok(routes)
                    })
                    .transpose()?;
                if let Some(default_routes) = &default_routes {
                    anyhow::ensure!(
                        default_route_count == Some(default_routes.len()),
                        "Source package default Node topology count does not match its modules"
                    );
                    anyhow::ensure!(
                        default_routes
                            .iter()
                            .all(|path| !topology.contains_key(path)),
                        "Source package Node topology assigns a module to default and named pools"
                    );
                }
                (topology, default_route_count, default_routes)
            },
            _ => anyhow::bail!("Source package Node pool topology is incomplete"),
        };
        let node_executor_pool_topology = NodeExecutorPoolTopology {
            assignments,
            default_route_count,
            default_routes,
        };
        Ok(Self {
            storage_key,
            sha256,
            external_deps_package_id: external_package_id,
            package_size,
            node_version,
            node_executor_pool_topology,
        })
    }
}

codegen_convex_serialization!(SourcePackage, SerializedSourcePackage);

#[cfg(test)]
mod tests {
    use super::*;

    fn serialized_source_package() -> SerializedSourcePackage {
        SerializedSourcePackage {
            storage_key: "package.zip".to_owned(),
            sha256: ByteBuf::from(vec![0; 32]),
            external_package_id: None,
            package_size: None,
            node_version: None,
            node_pool_module_paths: None,
            node_pool_names: None,
            node_pool_default_route_count: None,
            node_pool_default_module_paths: None,
        }
    }

    #[test]
    fn rejects_default_route_count_without_assignment_fields() {
        let old_package = SourcePackage::try_from(serialized_source_package()).unwrap();
        assert_eq!(
            old_package
                .node_executor_pool_topology
                .default_route_count(),
            None
        );

        let mut incomplete = serialized_source_package();
        incomplete.node_pool_default_route_count = Some(1);
        assert!(SourcePackage::try_from(incomplete).is_err());

        let mut original_pool_package = serialized_source_package();
        original_pool_package.node_pool_module_paths = Some(vec![]);
        original_pool_package.node_pool_names = Some(vec![]);
        SourcePackage::try_from(original_pool_package).unwrap();
    }

    #[test]
    fn exact_default_routes_round_trip_and_validate_count() {
        let topology = NodeExecutorPoolTopology::new_complete(
            BTreeMap::new(),
            ["ordinary.js".parse().unwrap()].into_iter().collect(),
        );
        let package = SourcePackage {
            storage_key: "package.zip".try_into().unwrap(),
            sha256: Sha256Digest::from([0; 32]),
            external_deps_package_id: None,
            package_size: PackageSize::default(),
            node_version: None,
            node_executor_pool_topology: topology.clone(),
        };
        let serialized = SerializedSourcePackage::try_from(package).unwrap();
        let round_tripped = SourcePackage::try_from(serialized).unwrap();
        assert_eq!(round_tripped.node_executor_pool_topology, topology);

        let mut invalid = serialized_source_package();
        invalid.node_pool_module_paths = Some(vec![]);
        invalid.node_pool_names = Some(vec![]);
        invalid.node_pool_default_route_count = Some(0);
        invalid.node_pool_default_module_paths = Some(vec!["ordinary.js".to_owned()]);
        assert!(SourcePackage::try_from(invalid).is_err());
    }
}
