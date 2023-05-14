using Microsoft.AnalysisServices.Tabular;
using Microsoft.SemanticKernel.Orchestration;
using Microsoft.SemanticKernel.SkillDefinition;

namespace SemanticKernel.Service.Skills;

public class PowerBITomSkill
{
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
}
