from soda.contracts.contract_verification import ContractVerification, ContractVerificationResult

# Exécuter la vérification du contrat
contract_verification_result: ContractVerificationResult = (
    ContractVerification.builder()
    .with_contract_yaml_file('yellow_taxi_contract.yml')  # Chemin vers le contrat
    .with_data_source_yaml_file('configuration.yml')  # Chemin vers la source de données
    .execute()
)

# Afficher les résultats de la vérification
print(str(contract_verification_result))

# Arrêter le pipeline en cas d'erreurs
if not contract_verification_result.is_ok():
    raise Exception("Échec de la vérification du contrat de données!")

