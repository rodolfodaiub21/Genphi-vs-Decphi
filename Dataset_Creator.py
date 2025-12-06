import pandas as pd

def read_csv_file(csv_file):
    return pd.read_csv(csv_file)

def main():
    emails_df=read_csv_file("Phishing_Email.csv")
    # Normalize and map email labels correctly: compare lowercase to lowercase
    emails_processed = pd.DataFrame({
        "Phishing Data": emails_df["Email Text"],
        "Phishing Type": "Mail",
        "Phishing Class": emails_df["Email Type"].apply(lambda x: 1 if str(x).strip().lower() == "phishing email" else 0)
    })
    url_df=read_csv_file("new_data_urls.csv")
    url_processed = pd.DataFrame({
        "Phishing Data": url_df['url'],
        "Phishing Type": "URL",
        # Coerce status to numeric and map to class: 1 -> phishing, else 0
        "Phishing Class": pd.to_numeric(url_df['status'], errors='coerce').fillna(0).astype(int).apply(lambda x: 1 if x == 1 else 0)

    })
    final_df = pd.concat([emails_processed,url_processed])
    final_df.to_csv("decphi_dataset.csv",index=False)
    print("Dataset Creado")
    print(final_df.head())
    # Print quick distribution sanity check
    try:
        print('\nClass distribution by Phishing Type:')
        print(final_df.groupby('Phishing Type')['Phishing Class'].value_counts())
    except Exception:
        pass
if __name__ == '__main__':
    main()  