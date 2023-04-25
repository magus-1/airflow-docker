import pandas as pd

ids = "./dags/raw/advertiser_ids"
df_ids = pd.read_csv(ids,header=0)

# Filtering ads views
ads = "./dags/raw/ads_views"
valid_ads = "./dags/files/valid_ads"

df_ads = pd.read_csv(ads,header=0)
df_output = (
    df_ads.merge(df_ids, 
            on=['advertiser_id'],
            how='left', 
            indicator=True)
    .query('_merge == "both"')
    .drop(columns='_merge')
)
df_output.to_csv(valid_ads, sep=',', header=True)

# Filtering products views
products = "./dags/raw/product_views"
valid_products = "./dags/files/valid_products"

df_products = pd.read_csv(products,header=0)
df_out = (
    df_products.merge(df_ids, 
            on=['advertiser_id'],
            how='left', 
            indicator=True)
)
print(df_out.head(100))

#df_out.to_csv(valid_products, sep=',', header=True)