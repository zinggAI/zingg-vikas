# Deterministic Matching - Zingg Enterprise Only Feature

Zingg Enterprise allows the ability to plug rule based deterministic matching along with already Zingg AI's probabilistic matching. If the data contains sure identifiers like emails, SSNs, passport ids etc, we can use these attributes to resolve records. The deterministic matching flow is weaved into Zingg's flow to ensure that each record which has a match finds one, probabilistically, deterministcally or both. If the data has known identifiers, Zingg Enterprise's deterministic matching highly improves both matching accuracy and performance.

Example for configuring it in json:

    "deterministicMatching":[
        {
           "matchCondition":[{"fieldName":"fname"},{"fieldName":"stNo"},{"fieldName":"add1"}]
        },
        {
           "matchCondition":[{"fieldName":"fname"},{"fieldName":"dob"},{"fieldName":"ssn"}]
        }, 
        {
           "matchCondition":[{"fieldName":"fname"},{"fieldName":"email"}]
        }
    ]



Python code example:

detMatchNameAdd = DeterministicMatching('fname','stNo','add1')
detMatchNameDobSsn = DeterministicMatching('fname','dob','ssn')
detMatchNameEmail = DeterministicMatching('fname','email')
args.setDeterministicMatchingCondition(detMatchNameAdd,detMatchNameDobSsn,detMatchNameEmail)