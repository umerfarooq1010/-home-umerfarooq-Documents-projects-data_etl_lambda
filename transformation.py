import numpy as np


def skewness(data):
    """calculate skew in data

    Args:
        data (pd.series): target column of the data as pandas series.

    Returns:
        float: skew value of the data
    """
    return data.skew()

def linear(data):
    """wrapper function for linear transformation

    Args:
        data (pd.series): target column of the data as pandas series.

    Returns:
        panda.series: return transformed data as pandas series.
    """
    return data

def exponent_3(data):
    """function for calculation of exponent 3 of the data

    Args:
        data (pd.series): target column of the data as pandas series.

    Returns:
        panda.series: return transformed data as pandas series.
    """
    return data**3

def power_t(data):
    """function for calculation of power transform of the data

    Args:
        data (pd.series): target column of the data as pandas series.

    Returns:
        nd.array: return transformed data as numpy array.
    """
    from sklearn.preprocessing import power_transform
    return power_transform(data.values.reshape(-1, 1), method='box-cox')

def quantile_t(data):
    """function for calculation of quantile transform of the data

    Args:
        data (pd.series): target column of the data as pandas series.

    Returns:
        nd.array: return transformed data as numpy array.
    """
    from sklearn.preprocessing import quantile_transform
    return quantile_transform(data.values.reshape(-1, 1), n_quantiles=10, random_state=0, copy=True)


def reciprocal(data):
    """function for calculation of reciprocal of the data

    Args:
        data (pd.series): target column of the data as pandas series.

    Returns:
        nd.array: return transformed data as numpy array.
    """
    return 1/data


TRANSFORMATIONS = {"linear": linear,
                   "log": np.log,
                   "sqrt": np.sqrt,
                   "square": np.square,
                   "exp_3": exponent_3,
                   "power_box_cox": power_t,
                   "quantile": quantile_t,
                   "reciprocal": reciprocal
                   }


def apply_deskew_transformation(data, input_dict):
    """This function takes in a dataframe along input dictionary, which contain feature to extract and column names.

    Args:
        date (pd.DataFrame): The date frame.
        input_dict (dict): name of the column.

    Returns:
        pandas.DataFrame: Dataframe with all the transformations applied.
    """

    target_column_name = input_dict["columns"]["target"]
    target_column = data[target_column_name]
    for transformation in input_dict["transformation"]:
        transformed = TRANSFORMATIONS[transformation](target_column)
        data[f"{target_column_name}_{transformation}"] = transformed
    return data


if __name__ == "__main__":
    import pandas as pd
    from sklearn.preprocessing import power_transform, quantile_transform

    data = pd.read_csv("/home/lap-0006/Projects/yhat_api/msft.csv")

    input_param = {
      "data_file_path": "/home/lap-0006/Projects/yhat_api/msft.csv",
      "columns": {
        "datetime": "Datetime",
        "target": "Volume"
      },
      "transformation": ["linear", "log", "sqrt", "square","exp_3","power_box_cox", "quantile", "reciprocal"]
    }

    data = apply_deskew_transformation(data, input_dict=input_param)

    l = len(input_param["transformation"])
    col = 3
    row = np.ceil(l / col)

    print(row, col)
    n = 1

    import matplotlib.pyplot as plt

    for trans in input_param["transformation"]:
        skew_val = skewness(data[trans])
        print("Skewness is :", skew_val)

        plt.subplot(row,col,n)
        plt.hist(data[trans])
        plt.title(trans + " : " + str(skew_val))
        n += 1

    plt.show()
