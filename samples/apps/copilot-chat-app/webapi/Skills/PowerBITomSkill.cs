using Microsoft.AnalysisServices.Tabular;
using Microsoft.SemanticKernel.Orchestration;
using Microsoft.SemanticKernel.SkillDefinition;

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
    }

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
        string promptTemplate = "";
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
}
