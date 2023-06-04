using System.Data.Common;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.SemanticKernel.Memory;
using Microsoft.SemanticKernel.Orchestration;
using Microsoft.SemanticKernel.SkillDefinition;
using static System.Net.WebRequestMethods;

namespace SemanticKernel.Service.Skills;

public class PowerBITomSkill
{
    /// <summary>
    /// <see cref="ContextVariables"/> parameter names.
    /// </summary>
    private static class TomSkillParameters
    {
        /// <summary>
        /// Dataset name
        /// </summary>
        public const string DatasetName = "datasetName";

        /// <summary>
        /// Workspace name
        /// </summary>
        public const string WorkspaceName = "workspaceName";

        /// <summary>
        /// Table name
        /// </summary>
        public const string TableName = "tableName";

        /// <summary>
        /// Column name
        /// </summary>
        public const string ColumnName = "columnName";

        /// <summary>
        /// Column Type name
        /// </summary>
        public const string ColumnTypeName = "columnTypeName";

        /// <summary>
        /// Power bi setting
        /// </summary>
        public const string SettingName = "settingName";
    }

    private static bool s_firstInvocation = true;

    private const string MemoryCollectionName = "PBIUrl";

    [SKFunction("Return dataset name of my power bi workspace")]
    [SKFunctionInput(Description = "Workspace name to search in power bi.")]
    public string GetDatasetName(string workspaceName)
    {
        string workspaceConnection = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        string connectString = $"DataSource={workspaceConnection};";
        using (Server server = new Server())
        {
            server.Connect(connectString);

            string promptTemplate = "Dataset name list:";
            foreach (Database database in server.Databases)
            {
                promptTemplate += database.Name;
            }
            return $"The dataset names for user Po-wei Huang is {promptTemplate} for {workspaceName}.";
        }
    }

    [SKFunction("Given workspace name and dataset name, return dataset info of a power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.WorkspaceName, Description = "Workspace name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.DatasetName, Description = "Dataset name for power bi workspace")]
    public string GetDatasetInformation(SKContext context)
    {
        if (!context.Variables.Get(TomSkillParameters.DatasetName, out string datasetName))
        {
            context.Fail($"Missing variable {TomSkillParameters.DatasetName}.");
            return $"Input insufficient. No {TomSkillParameters.DatasetName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.WorkspaceName, out string workspaceName))
        {
            context.Fail($"Missing variable {TomSkillParameters.WorkspaceName}.");
            return $"Input insufficient. No {TomSkillParameters.WorkspaceName}.";
        }

        string workspaceConnection = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        string connectString = $"DataSource={workspaceConnection};";
        string promptTemplate = "";
        using (Server server = new Server())
        {
            server.Connect(connectString);

            string targetDatabaseName = datasetName;
            Database database = server.Databases.GetByName(targetDatabaseName);

            promptTemplate += ("Name: " + database.Name);
            promptTemplate += ("ID: " + database.ID);
            promptTemplate += ("ModelType: " + database.ModelType);
            promptTemplate += ("CompatibilityLevel: " + database.CompatibilityLevel);
            promptTemplate += ("LastUpdated: " + database.LastUpdate);
            promptTemplate += ("EstimatedSize: " + database.EstimatedSize);
            promptTemplate +=("CompatibilityMode: " + database.CompatibilityMode);
            promptTemplate +=("LastProcessed: " + database.LastProcessed);
            promptTemplate += ("LastSchemaUpdate: " + database.LastSchemaUpdate);
        }

        return promptTemplate;
    }

    [SKFunction("Given workspace name, dataset name and table name, return table schema.")]
    [SKFunctionContextParameter(Name = TomSkillParameters.WorkspaceName, Description = "Workspace name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.DatasetName, Description = "Dataset name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.TableName, Description = "Table name for power bi workspace")]
    public string GetTableSchema(SKContext context)
    {
        if (!context.Variables.Get(TomSkillParameters.DatasetName, out string datasetName))
        {
            context.Fail($"Missing variable {TomSkillParameters.DatasetName}.");
            return $"Input insufficient. No {TomSkillParameters.DatasetName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.WorkspaceName, out string workspaceName))
        {
            context.Fail($"Missing variable {TomSkillParameters.WorkspaceName}.");
            return $"Input insufficient. No {TomSkillParameters.WorkspaceName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.TableName, out string tableName))
        {
            context.Fail($"Missing variable {TomSkillParameters.TableName}.");
            return $"Input insufficient. No {TomSkillParameters.TableName}.";
        }

        string workspaceConnection = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        string connectString = $"DataSource={workspaceConnection};";
        string promptTemplate = $"Table schema for table {tableName}, dataset name {datasetName}, workspace name {workspaceName}:"; ;
        using (Server server = new Server())
        {
            server.Connect(connectString);

            string targetDatabaseName = datasetName;
            Database database = server.Databases.GetByName(targetDatabaseName);
            Table table = database.Model.Tables.Find(tableName);

            foreach (Column column in table.Columns)
            {
                promptTemplate += ("\nCoulumn: " + column.Name);
                promptTemplate += ("\nType: " + column.DataType);
            }
        }

        return promptTemplate;
    }

    [SKFunction("Given workspace name, dataset name and table name, return M program of table.")]
    [SKFunctionContextParameter(Name = TomSkillParameters.WorkspaceName, Description = "Workspace name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.DatasetName, Description = "Dataset name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.TableName, Description = "Table name for power bi workspace")]
    public string GetMProgram(SKContext context)
    {
        if (!context.Variables.Get(TomSkillParameters.DatasetName, out string datasetName))
        {
            context.Fail($"Missing variable {TomSkillParameters.DatasetName}.");
            return $"Input insufficient. No {TomSkillParameters.DatasetName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.WorkspaceName, out string workspaceName))
        {
            context.Fail($"Missing variable {TomSkillParameters.WorkspaceName}.");
            return $"Input insufficient. No {TomSkillParameters.WorkspaceName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.TableName, out string tableName))
        {
            context.Fail($"Missing variable {TomSkillParameters.TableName}.");
            return $"Input insufficient. No {TomSkillParameters.TableName}.";
        }

        string workspaceConnection = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        string connectString = $"DataSource={workspaceConnection};";
        string promptTemplate = "";
        using (Server server = new Server())
        {
            server.Connect(connectString);

            string targetDatabaseName = datasetName;
            Database database = server.Databases.GetByName(targetDatabaseName);
            Table table = database.Model.Tables.Find(tableName);

            foreach (Partition partition in table.Partitions)
            {
                if (partition.SourceType == PartitionSourceType.M)
                {
                    var source = (MPartitionSource) partition.Source;
                    string prompt = $"The M program for table {tableName}, dataset name {datasetName}, workspace name {workspaceName} is:";
                    return prompt + source.Expression + "\n.";
                }    
            }

        }

        return promptTemplate;
    }

    [SKFunction("Given workspace name, dataset name and table name, alter a column to target type.")]
    [SKFunctionContextParameter(Name = TomSkillParameters.WorkspaceName, Description = "Workspace name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.DatasetName, Description = "Dataset name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.TableName, Description = "Table name")]
    [SKFunctionContextParameter(Name = TomSkillParameters.ColumnName, Description = "Column name")]
    [SKFunctionContextParameter(Name = TomSkillParameters.ColumnTypeName, Description = "Target type to alter")]
    public string AlterColumnType(SKContext context)
    {
        if (!context.Variables.Get(TomSkillParameters.DatasetName, out string datasetName))
        {
            context.Fail($"Missing variable {TomSkillParameters.DatasetName}.");
            return $"Input insufficient. No {TomSkillParameters.DatasetName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.WorkspaceName, out string workspaceName))
        {
            context.Fail($"Missing variable {TomSkillParameters.WorkspaceName}.");
            return $"Input insufficient. No {TomSkillParameters.WorkspaceName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.TableName, out string tableName))
        {
            context.Fail($"Missing variable {TomSkillParameters.TableName}.");
            return $"Input insufficient. No {TomSkillParameters.TableName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.ColumnName, out string columnName))
        {
            context.Fail($"Missing variable {TomSkillParameters.ColumnName}.");
            return $"Input insufficient. No {TomSkillParameters.ColumnName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.ColumnTypeName, out string ColumnTypeName))
        {
            context.Fail($"Missing variable {TomSkillParameters.ColumnTypeName}.");
            return $"Input insufficient. No {TomSkillParameters.ColumnTypeName}.";
        }

        string workspaceConnection = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        string connectString = $"DataSource={workspaceConnection};";

        using (Server server = new Server())
        {
            server.Connect(connectString);

            string targetDatabaseName = datasetName;
            Database database = server.Databases.GetByName(targetDatabaseName);
            Table table = database.Model.Tables.Find(tableName);
            Column column = table.Columns[columnName];
            column.DataType = DataType.Decimal;

            database.Model.SaveChanges();
        }

        return $"Column type altered to decimal successfully for column {columnName}, table {tableName}, dataset name {datasetName}, workspace name {workspaceName}.";
    }

    [SKFunction("Given workspace name and dataset name, return dataset problem and suggestions.")]
    [SKFunctionContextParameter(Name = TomSkillParameters.WorkspaceName, Description = "Workspace name for power bi workspace")]
    [SKFunctionContextParameter(Name = TomSkillParameters.DatasetName, Description = "Dataset name for power bi workspace")]
    public string GetDatasetProblemAndSuggestion(SKContext context)
    {
        if (!context.Variables.Get(TomSkillParameters.DatasetName, out string datasetName))
        {
            context.Fail($"Missing variable {TomSkillParameters.DatasetName}.");
            return $"Input insufficient. No {TomSkillParameters.DatasetName}.";
        }

        if (!context.Variables.Get(TomSkillParameters.WorkspaceName, out string workspaceName))
        {
            context.Fail($"Missing variable {TomSkillParameters.WorkspaceName}.");
            return $"Input insufficient. No {TomSkillParameters.WorkspaceName}.";
        }

        string workspaceConnection = $"powerbi://api.powerbi.com/v1.0/myorg/{workspaceName}";
        string connectString = $"DataSource={workspaceConnection};";
        string promptTemplate = "";
        using (Server server = new Server())
        {
            server.Connect(connectString);

            string targetDatabaseName = datasetName;
            Database database = server.Databases.GetByName(targetDatabaseName);

            foreach (Table table in database.Model.Tables)
            {
                foreach (Column column in table.Columns)
                {
                    if (column.DataType == DataType.Double)
                    {
                        promptTemplate += $"The model is problematic becasue column type is double for column {column.Name}, table {table.Name}, dataset name {datasetName}, workspace name {workspaceName}. Make it decimal.";
                        return promptTemplate;
                    }
                }
            }
        }

        return $"Everything is perfect for dataset name {datasetName}, workspace name {workspaceName}.";
    }

    [SKFunction("For a setting of power bi, get url link of setting page to help cx.")]
    [SKFunctionName("GetLinkFromTopic")]
    [SKFunctionContextParameter(Name = TomSkillParameters.SettingName, Description = "Setting name that user want to find.")]
    public async Task<string> GetLinkFromTopicAsync(SKContext context)
    {
        // Check if it's the first invocation
        if (s_firstInvocation)
        {
            // Perform the action you want to do only on the first invocation
            Console.WriteLine("This is the first invocation!");
            try
            {
                // Set the flag variable to false to indicate that the first invocation has already occurred
                await context.Memory.SaveInformationAsync(
                    MemoryCollectionName,
                    "Large dataset storage format",
                    "0",
                    "https://msit.powerbi.com/groups/workspaceId/settings/datasets?ctid=datasetId&openReportSource=SubscribeOthers&experience=power-bi"
                    );
            }
            catch
            {
                Console.WriteLine("Some error happened.");
            }

            s_firstInvocation = false;
        }

        if (!context.Variables.Get(TomSkillParameters.SettingName, out string settingName))
        {
            context.Fail($"Missing variable {TomSkillParameters.DatasetName}.");
            return $"Input insufficient. No {TomSkillParameters.DatasetName}.";
        }

        var results = context.Memory.SearchAsync(MemoryCollectionName, settingName, limit: 1, minRelevanceScore: 0.77);

        await foreach (MemoryQueryResult r in results)
        {
            return $"The url for setting {settingName} is https://msit.powerbi.com/groups/1c27cb72-99ab-4bb4-b58d-de31b5282580/settings/datasets/b4b40cdb-d3e7-4623-a7a4-bbf0390f1ed6?experience=power-bi";
        }

        return $"I couldn't find related setting.";
    }

}
