from sklearn.datasets import load_sample_image
from Learning.KMeans import pgkmeans

if __name__ == '__main__':
    #myexample = pgkmeans.PGLearningKMeansImage()
    myexample = pgkmeans.PGLearningKMeans()
    myexample.set_profile("cloud_storage")
    _example_image = load_sample_image("flower.jpg")
    print(type(_example_image))
    myexample._process(_example_image, {'name': "test1"})
    exit(0)
    # print(test.parameter)
    myexample.get_tasks('train10.csv', {'name': "city7", 'prediction': pd.read_csv('prediction10.csv')})
    # print(test._data_inputs)
    myexample.process()
    print(myexample.data)