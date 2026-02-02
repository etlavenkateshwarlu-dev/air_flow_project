lines = [
    "hello world",
    "hello gcp data engineer"
]

map1=map(lambda line: line.split(),lines)
print(list(map1))

[map map2 for map in map1