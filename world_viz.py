import pandas as pd
import numpy as np

df = pd.read_csv('data/dataset_2.csv')
df_pivot = pd.read_csv('data/dataset_2_pivot.csv')
corr_df = pd.read_csv('data/correlations.csv')
corr_df_pivot = pd.read_csv('data/correlations_pivot.csv')
types_df = pd.read_csv('data/metric_types.csv')

# initiate world_df
world_df = df_pivot

# join in types data
world_df = df_pivot.merge(types_df, how = 'inner', on='metric')

# latest metric in group
world_df["latest_metric"] = world_df.groupby(["country","metric_type","metric_no_year"])["year_2025_if latest"].rank(method="first", ascending=False)
world_df["latest_metric"] = np.where(world_df["latest_metric"]==1,world_df["latest_metric"],0)

# create ranks depending on high/low
world_df["metric_rank_high"] = world_df.groupby(["metric"])["value"].rank(method="min", ascending=False)
world_df["metric_rank_low"] = world_df.groupby(["metric"])["value"].rank(method="min", ascending=True)
world_df["metric_rank"] = np.where(world_df['ranking'] == 'high',world_df["metric_rank_high"],world_df["metric_rank_low"])

# remove working columns
world_df = world_df.drop(columns=['metric_rank_high', 'metric_rank_low'])

# countries best and worst factors
world_df["country_strengths_order"] = world_df.groupby(["country","metric_type"])["metric_rank"].rank(method="dense", ascending=True)
world_df["country_weaknesses_order"] = world_df.groupby(["country","metric_type"])["metric_rank"].rank(method="dense", ascending=False)

world_df["country_strengths_order"] = world_df.groupby(["country","metric_type"])["country_strengths_order"].rank(method="first", ascending=True)
world_df["country_weaknesses_order"] = world_df.groupby(["country","metric_type"])["country_weaknesses_order"].rank(method="first", ascending=True)

# countries best and worst factors by metric groups
world_df["country_strengths_order_by_broad_metric"] = world_df.groupby(["country","metric_type","metric_no_year"])["country_strengths_order"].rank(method="dense", ascending=True)
world_df["country_strengths_order_by_broad_metric"] = np.where(world_df["country_strengths_order_by_broad_metric"]==1,world_df["country_strengths_order"],None)
world_df["country_strengths_order_by_broad_metric"] = world_df.groupby(["country","metric_type"])["country_strengths_order_by_broad_metric"].rank(method="dense", ascending=True)

world_df["country_weaknesses_order_by_broad_metric"] = world_df.groupby(["country","metric_type","metric_no_year"])["country_weaknesses_order"].rank(method="dense", ascending=True)
world_df["country_weaknesses_order_by_broad_metric"] = np.where(world_df["country_weaknesses_order_by_broad_metric"]==1,world_df["country_weaknesses_order"],None)
world_df["country_weaknesses_order_by_broad_metric"] = world_df.groupby(["country","metric_type"])["country_weaknesses_order_by_broad_metric"].rank(method="dense", ascending=True)

# build in reccomendations
recc_df = df_pivot.merge(types_df, how = 'inner', on='metric')

# latest metric in group
recc_df["latest_metric"] = recc_df.groupby(["country","metric_type","metric_no_year"])["year_2025_if latest"].rank(method="first", ascending=False)
recc_df["latest_metric"] = np.where(recc_df["latest_metric"]==1,recc_df["latest_metric"],0)

# create target_metric
target_df = recc_df[['country','metric','metric_no_year','metric_type','latest_metric','value']]
target_df.columns = ['country','target_metric','target_metric_no_year','target_metric_type','latest_target_metric','target_value']

recc_df = recc_df.merge(target_df, how = 'inner', on = 'country')

recc_df = recc_df.loc[recc_df['metric_no_year'] != recc_df['target_metric_no_year']]
recc_df = recc_df.loc[recc_df['metric_type'] != recc_df['target_metric_type']]

recc_df = recc_df.loc[recc_df['metric_type'] != 'descriptive']
recc_df = recc_df.loc[recc_df['target_metric_type'] != 'descriptive']


recc_df = recc_df.merge(corr_df_pivot, how = 'inner', on = ['metric','target_metric'])


#recc_df["correlation_avg_broad_to_broad"] = recc_df.groupby(["country","metric_no_year","target_metric_no_year"])["correlation"].mean()

print(recc_df)



#world_df["metric_rank_high"] = world_df.groupby(["country"])["value"].rank(method="min", ascending=False, inplace = True)


# cross_join = df_pivot[['country','metric','value']]
# world_df.columns = ['country', 'iso_country_code_2018','metric','value','target_metric','target_value']

# world_df = df_pivot.merge(cross_join, how = 'inner', on='country')
# world_df.columns = ['country', 'iso_country_code_2018','metric','value','target_metric','target_value']

# world_df = world_df.merge(corr_df_pivot, how = 'inner', on=['metric','target_metric'])
# world_df['correlation_absolute'] = world_df['correlation'].abs()

# world_df = world_df.merge(types_df, how = 'inner', on='metric')

# types_df.columns = ['target_metric','target_metric_type','target_latest','target_metric_no_year','target_ranking']
# world_df = world_df.merge(types_df, how = 'inner', on='target_metric')

# world_df["metric_rank_high"] = world_df.groupby(["metric","target_metric"])["value"].rank(method="min", ascending=False)
# world_df["metric_rank_low"] = world_df.groupby(["metric","target_metric"])["value"].rank(method="min", ascending=True)
# world_df["metric_rank"] = np.where(world_df['ranking'] == 'high',world_df["metric_rank_high"],world_df["metric_rank_low"])

# world_df["target_metric_rank_high"] = world_df.groupby(["target_metric","metric"])["target_value"].rank(method="min", ascending=False)
# world_df["target_metric_rank_low"] = world_df.groupby(["target_metric","metric"])["target_value"].rank(method="min", ascending=True)
# world_df["target_metric_rank"] = np.where(world_df['target_ranking'] == 'high',world_df["target_metric_rank_high"],world_df["target_metric_rank_low"])

# world_df = world_df.drop(columns=['metric_rank_high', 'metric_rank_low','target_metric_rank_high', 'target_metric_rank_low'])

# world_df = world_df.loc[world_df['metric_no_year'] != world_df['target_metric_no_year']]
# world_df = world_df.loc[world_df['metric_type'] != world_df['target_metric_type']]

# world_df = world_df.drop_duplicates()

# latest_metric_df = world_df.loc[world_df['latest'] == 1]
# latest_metric_df = latest_metric_df[["country","metric_type","metric_no_year","metric_rank"]]
# latest_metric_df.columns = ["country","metric_type","metric_no_year","latest_rank"]
# #world_df = world_df.merge(latest_metric_df, how = 'inner', on=["country","metric_type","metric_no_year"])

# latest_metric_df = latest_metric_df.drop_duplicates()
# latest_metric_df.to_csv('data/latest_metric_df.csv', index = False)

#world_df["country_strengths_order"] = world_df.groupby(["country","metric_type"])["latest_rank"].rank(method="dense", ascending=True)
#world_df["country_weaknesses_order"] = world_df.groupby(["country","metric_type"])["latest_rank"].rank(method="dense", ascending=False)

print(world_df)
world_df = world_df.drop_duplicates()
world_df.to_csv('data/world_df.csv', index = False)

recc_df.to_csv('data/recommendations_df.csv', index = False)