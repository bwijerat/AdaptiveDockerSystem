import numpy as np


def regularized_lin_regression(design_matrix, target_matrix, regularizer):
    #Since we want to use the numpy lstsq method
    #The function we want is of the form a x = b
    #Where we solve for x in terms of a and b
    #Assert that we can rewrite the regularized least squares regression as (X^T * X + lambda * I) w = X^T y
    #Thus a = (X^T * X + lambda * I), x = w, b = X^T * y
    #Start by calculating b
    b = np.matmul(design_matrix.transpose(), target_matrix)
    #Start calculating a
    a = np.matmul(design_matrix.transpose(), design_matrix)
    #a is now a square matrix so we can use any value in a.shape for indentity matrix
    a = a + (regularizer * np.identity(a.shape[0]))
    #solve for w and return
    w = np.linalg.lstsq(a, b, rcond=None)[0]
    return w
    