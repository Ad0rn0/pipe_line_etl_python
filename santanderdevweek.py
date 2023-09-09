# ETL - Envio de mensagens para alunos que não pagaram a mensalidade da faculdade. O programa lerá um .csv contendo os dados dos alunos e preencherá a coluna Message com uma mensagem para cada aluno que não pagou a mensalidade e outra para cada aluno que já efetuou o pagamento.
import pandas as pd

# Extract
def extract_data():
    df = pd.read_csv('SDW2023.csv', dtype={'Message': str,'PaymentStatus': str})
    users_ids = df['UserID'].tolist()
    users_names = df['UserName'].tolist()
    return users_ids, users_names, df

# Transform
def transform_data(users_ids, users_names, df):
    for id in users_ids:
        if df.at[id-1,'PaymentStatus'] != 'Pago':
            df.at[id-1, 'Message'] = f'Olá, {users_names[id-1]}. Você ainda não pagou a mensalidade deste mês, cuidado para não pagar mais juros!'
        else:
            df.at[id-1, 'Message'] = None
    
# Load
def load_data():
    df.to_csv('SDW2023.csv', index=False)

users_ids, users_names, df = extract_data()
transform_data(users_ids, users_names, df)
load_data()

