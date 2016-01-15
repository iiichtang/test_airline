# encoding=utf-8
print(__doc__)

from time import time
import numpy as np
import matplotlib.pyplot as plt
import csv

from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale

data_list = list()
data_list2 = list()
old_x_max = 0
old_x_min = 99999
old_y_max = 0
old_y_min = 99999
index_max = 0
n_digits = 6

f = open('customer_point_2.csv', 'r')
for row in csv.reader(f):
    if index_max == 0:
        old_x_max = float(row[1])
        old_x_min = float(row[1])
        old_y_max = float(row[2])
        old_y_min = float(row[2])
        index_max = 1
    else:
        if float(row[1]) > old_x_max:
            old_x_max = float(row[1])
        elif float(row[1]) < old_x_min:
            old_x_min = float(row[1])
        if float(row[2]) > old_y_max:
            old_y_max = float(row[2])
        elif float(row[2]) < old_y_min:
            old_y_min = float(row[2])
    data_list.append([row[1], row[2]])
f.close()

print old_x_max
print old_x_min
print old_y_max
print old_y_min
OldxRange = old_x_max - old_x_min
OldyRange = old_y_max - old_y_min
NewRange = (100 - 0)

for data in data_list:
    data_list2.append([(((float(data[0]) - old_x_min) * NewRange) / OldxRange),
                       (((float(data[1]) - old_y_min) * NewRange) / OldyRange)])




reduced_data = np.array(data_list2)

###############################################################################
# Visualize the results on PCA-reduced data

#reduced_data = PCA(n_components=2).fit_transform(data)
kmeans = KMeans(init='k-means++', n_clusters=n_digits, n_init=10)
kmeans.fit(reduced_data)
print "kmean"
# Step size of the mesh. Decrease to increase the quality of the VQ.
h = .02     # point in the mesh [x_min, m_max]x[y_min, y_max].

# Plot the decision boundary. For that, we will assign a color to each
x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1
xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

print xx.min()
print xx.max()
print yy.min()
print yy.max()

# Obtain labels for each point in mesh. Use last trained model.
Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])

# Put the result into a color plot
Z = Z.reshape(xx.shape)
print"Z: ", Z
plt.figure(1)
plt.clf()
plt.imshow(Z, interpolation='nearest',
           extent=(xx.min(), xx.max(), yy.min(), yy.max()),
           cmap=plt.cm.Paired,
           aspect='auto', origin='lower')

#plot points
plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=2)
#plt.scatter(reduced_data[:, 0], reduced_data[:, 1])

# Plot the centroids as a white X
centroids = kmeans.cluster_centers_
plt.scatter(centroids[:, 0], centroids[:, 1],
            marker='x', s=169, linewidths=3,
            color='w', zorder=10)
plt.title('K-means clustering on the digits dataset (PCA-reduced data)\n'
          'Centroids are marked with white cross')
plt.xlim(x_min, x_max)
plt.ylim(y_min, y_max)
plt.xticks(())
plt.yticks(())
plt.show()