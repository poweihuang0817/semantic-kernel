using Microsoft.SemanticKernel.SkillDefinition;
using Microsoft.SemanticKernel.Orchestration;

namespace SemanticKernel.Service.Skills;

public class PowerBITomSkill
{
    [SKFunction("Return table name of my power bi dataset.")]
    public string GetTableName()
    {
        return "PowerBIHackathon";
    }
}
