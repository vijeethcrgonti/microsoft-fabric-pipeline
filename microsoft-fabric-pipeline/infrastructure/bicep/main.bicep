// main.bicep  —  infrastructure/bicep/
// Provisions Azure resources supporting the Microsoft Fabric pipeline:
// - Resource Group
// - Fabric Capacity (F64 SKU)
// - Azure Key Vault (secrets for Service Principal)
// - Storage Account (ADLS Gen2 for legacy file landing)
// - Azure SQL Database (source system simulation)
// - Log Analytics Workspace (monitoring)

targetScope = 'resourceGroup'

@description('Deployment environment')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Azure region')
param location string = resourceGroup().location

@description('Fabric capacity admin email')
param fabricAdminEmail string

@description('SQL admin login')
param sqlAdminLogin string

@secure()
@description('SQL admin password')
param sqlAdminPassword string

var prefix = 'fabric-retail'
var tags = {
  project: 'microsoft-fabric-pipeline'
  environment: environment
  managedBy: 'bicep'
}

// ── Log Analytics Workspace ─────────────────────────────────────────────────

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${prefix}-logs-${environment}'
  location: location
  tags: tags
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
    features: { enableLogAccessUsingOnlyResourcePermissions: true }
  }
}

// ── Key Vault ───────────────────────────────────────────────────────────────

resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: '${prefix}-kv-${environment}'
  location: location
  tags: tags
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: tenant().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
  }
}

// ── ADLS Gen2 (legacy file landing zone) ───────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: replace('${prefix}adls${environment}', '-', '')
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    encryption: {
      services: {
        blob: { enabled: true }
        file: { enabled: true }
      }
      keySource: 'Microsoft.Storage'
    }
  }
}

resource landingContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageAccount.name}/default/landing'
  properties: { publicAccess: 'None' }
}

// ── Azure SQL Database (source system) ─────────────────────────────────────

resource sqlServer 'Microsoft.Sql/servers@2022-11-01-preview' = {
  name: '${prefix}-sql-${environment}'
  location: location
  tags: tags
  properties: {
    administratorLogin: sqlAdminLogin
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2022-11-01-preview' = {
  parent: sqlServer
  name: 'orders_db'
  location: location
  tags: tags
  sku: { name: 'GP_S_Gen5', tier: 'GeneralPurpose', capacity: 1 }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    autoPauseDelay: 60
    minCapacity: '0.5'
    requestedBackupStorageRedundancy: 'Local'
  }
}

// ── Fabric Capacity ─────────────────────────────────────────────────────────

resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = {
  name: '${prefix}-capacity-${environment}'
  location: location
  tags: tags
  sku: {
    name: environment == 'prod' ? 'F64' : 'F2'
    tier: 'Fabric'
  }
  properties: {
    administration: {
      members: [fabricAdminEmail]
    }
  }
}

// ── Outputs ─────────────────────────────────────────────────────────────────

output fabricCapacityId string = fabricCapacity.id
output storageAccountName string = storageAccount.name
output keyVaultName string = keyVault.name
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output logAnalyticsWorkspaceId string = logAnalytics.id
