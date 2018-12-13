import numpy as np


#Kalman filter to estimate the number of containers needed
#State values x are sql_conts_cpu, web_work_conts_cpu, sql_conts_mem, web_work_conts_mem
class kalmanEstimator():
    def __init__(self, state_matrix, control_matrix, init_x):
        self.F = state_matrix
        self.B = control_matrix
        #Filter will estimate values of all inputs
        #Thus we create a column vector of 4 values
        self.x = init_x
        self.Q = 0.002 * np.random.randn(4, 4)
        self.R = np.zeros((4, 4))
        self.P = np.zeros((4, 4))
        self.K = np.zeros((4, 4))
    def predict(self, control_vector):
        #Predicted state estimate is given by: state_matrix * x[t - 1] + control_matrix * observations + w[t]
        self.x = np.matmul(self.F, self.x) + np.matmul(self.B, control_vector)
        #Predicted error convariance P is given by: F*P*F.transpose + q[t]
        self.P = np.matmul(np.matmul(self.F, self.P),self.F.transpose()) + self.Q
    def estimate(self, control_vector):
        #Gives an estimate of x without changing internal values
        return np.matmul(self.F, self.x) + np.matmul(self.B, control_vector)
    def update_B(self, new_B):
        self.B = new_B
    def update(self, observation, observation_covariance):
        #Update Kalman Filter properties
        #Get new innovation
        y = observation - self.x
        #Get innovation covariance
        self.R = observation_covariance
        S = self.R + self.P
        #Get Kalman gain
        #The formula for Kalman gain is given as K = P * S^-1
        #In general we should always avoid solving for the inverse of a matrix
        #Thus we do some basic lin alg and rewrite the Kalman gain equation as K * S = P
        #Alright that didn't help so screw it we solve for the inverse
        self.K = np.matmul(self.P, np.linalg.inv(S))
        #update state estimate
        self.x = self.x + np.matmul(self.K, y)
        #Update P
        self.P = np.matmul(np.matmul((np.identity(4) - self.K), self.P), (np.identity(4)-self.K).transpose()) + self.R