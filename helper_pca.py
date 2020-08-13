import pandas as pd
import numpy as np
from sklearn.decomposition import PCA

#plots
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")
sns.set_context("talk")

def do_pca(data_std, feature_names=None, top_k=10, top_pc=10):
    """Conduct a PCA on standardized data. Show scree plot and heatmap of factor loadings. 
        Returns a PCA object and the PCA tranformation of the data.
     
    Input
        data_std: dataframe containing standardized data
        feature_names: list of features 
        top_k: Top-k variables to list for factor loadings
        top_pc: Show PC up to top_pc in scree plot and heatmap 
        
    Return 
        tuple: (pca object, pca transformation)
    """
    # store feature names if not given
    if feature_names is None:
        feature_names = list(data_std.columns)
    
    #---------------
    ### 1.
    # initialize and compute pca
    pca = PCA()
    X_pca = pca.fit_transform(data_std)
    
    
    #---------------
    ### 2.
    # print explained variance for each component
    n_components = len(pca.explained_variance_ratio_)
    explained_variance = pca.explained_variance_ratio_
    cum_explained_variance = np.cumsum(explained_variance)
    idx = np.arange(n_components)+1

    df_explained_variance = pd.DataFrame([explained_variance, cum_explained_variance], 
                                         index=['explained variance', 'cumulative'], 
                                         columns=idx).T

    mean_explained_variance = df_explained_variance.iloc[:,0].mean() # calculate mean explained variance

    print('PCA Overview')
    print('='*40)
    print("Total: {} components".format(n_components))
    print('-'*40)
    print('Mean explained variance:', round(mean_explained_variance,3))
    print('-'*40)
    print(df_explained_variance.head(20))
    print('-'*40)
    
    
    #---------------
    ### 3. 
    # Explained variance plot (scree plot)
    df_explained_variance_limited = df_explained_variance.iloc[:top_pc,:]

    fig, ax1 = plt.subplots(figsize=(15,6))

    ax1.set_title('Explained variance across principal components', fontsize=14)
    ax1.set_xlabel('Principal component', fontsize=12)
    ax1.set_ylabel('Explained variance', fontsize=12)

    ax2 = sns.barplot(x=idx[:top_pc], y='explained variance', data=df_explained_variance_limited, palette='YlGnBu')
    ax2 = ax1.twinx()
    ax2.grid(False)

    ax2.set_ylabel('Cumulative', fontsize=14)
    ax2 = sns.lineplot(x=idx[:top_pc]-1, y='cumulative', data=df_explained_variance_limited, color='#fc8d59')

    ax1.axhline(mean_explained_variance, ls='--', color='#fc8d59') #plot mean
    #label y axis
    ax1.text(-.8, mean_explained_variance+(mean_explained_variance*.05), "average", color='#fc8d59', fontsize=14) 

    max_y1 = max(df_explained_variance_limited.iloc[:,0])
    max_y2 = max(df_explained_variance_limited.iloc[:,1])
    ax1.set(ylim=(0, max_y1+max_y1*.1))
    ax2.set(ylim=(0, max_y2+max_y2*.1))

    plt.show()
    
    
    #---------------
    ### 4. 
    # Correlations of features with components
    df_c = pd.DataFrame(pca.components_, columns=feature_names).T

    print("Factor Loadings of 1st PC")
    print('='*40,'\n')
    print('Top {} highest'.format(top_k))
    print('-'*40)
    print(df_c.iloc[:,0].sort_values(ascending=False)[:top_k], '\n')

    print('Top {} lowest'.format(top_k))
    print('-'*40)
    print(df_c.iloc[:,0].sort_values()[:top_k])
    
    # Plot heatmap
    size_xaxis = round(top_pc * 1.5)
    size_yaxis = round(n_components * 0.5)

    fig, ax = plt.subplots(figsize=(size_xaxis,size_yaxis))
    sns.heatmap(df_c.iloc[:,:top_pc], annot=True, cmap="YlGnBu", ax=ax)
    plt.show()
    
    #---------------
    ### 5.
    # Pca object and transformed data
    return pca, pd.DataFrame(X_pca)