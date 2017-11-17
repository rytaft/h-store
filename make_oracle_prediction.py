
prediction = []
with open('/data/rytaft/actual_load_5min.txt') as f:
    prediction = f.read().splitlines() 

with open('/data/rytaft/predpoints_forecastwindow_60_oracle.txt', 'w') as f:
    while len(prediction) > 0:
        f.write(" ".join(prediction[0:61]))
        f.write("\n")
        prediction.pop(0)
