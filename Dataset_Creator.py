import pandas as pd

def read_csv_file(csv_file):
    return pd.read_csv(csv_file)

def main():
    emails_df=read_csv_file("Phishing_Email.csv")
    emails_processed = pd.DataFrame({
        "Phishing Data":emails_df["Email Text"],
        "Phishing Type": "Mail",
        "Phishing Class" : emails_df["Email Type"].apply(lambda x:1 if x.lower()=="Phishing Email" else 0)
    })
    url_df=read_csv_file("new_data_urls.csv")
    url_processed = pd.DataFrame({
        "Phishing Data": url_df['url'],
        "Phishing Type": "URL",
        "Phishing Class": url_df['status'].apply(lambda x:1 if x == 0 else 1)

    })
    final_df = pd.concat([emails_processed,url_processed])
    final_df.to_csv("decphi_dataset.csv",index=False)
    print("Dataset Creado")
    print(final_df.head())
if __name__ == '__main__':
    main()  