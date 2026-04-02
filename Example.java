void main() {
  IO.println("# SimpleReduce");
  simpleReduce(Stream.of("A", "B", "C", "D", "E", "F", "G"));
  IO.println("");
  IO.println("# ReduceWithCombiner");
  reduceWithCombiner(Stream.of('A', 'B', 'C', 'D', 'E', 'F', 'G'));
  IO.println("");
  IO.println("# SimpleReduce (parallel)");
  simpleReduce(Stream.of("A", "B", "C", "D", "E", "F", "G").parallel());
  IO.println("");
  IO.println("# ReduceWithCombiner (parallel)");
  reduceWithCombiner(Stream.of('A', 'B', 'C', 'D', 'E', 'F', 'G').parallel());
  IO.println("");
  IO.println("# Fold");
  fold(Stream.of('A', 'B', 'C', 'D', 'E', 'F', 'G'));
  IO.println("");
  IO.println("# Fold (parallel)");
  fold(Stream.of('A', 'B', 'C', 'D', 'E', 'F', 'G').parallel());
  IO.println("");
  IO.println("");
  IO.println("# Average sensor values");
  IO.println("# Reduce");
  weightedAverageReduce(Stream.of(500.0, 750.0, 900.0, 1200.0, 1100.0, 950.0));
  IO.println("");
  IO.println("# Reduce (parallel)");
  weightedAverageReduce(Stream.of(500.0, 750.0, 900.0, 1200.0, 1100.0, 950.0).parallel());
  IO.println("");
  IO.println("# Fold");
  weightedAverageFold(Stream.of(500.0, 750.0, 900.0, 1200.0, 1100.0, 950.0));
  IO.println("");
  IO.println("# Fold (parallel)");
  weightedAverageFold(Stream.of(500.0, 750.0, 900.0, 1200.0, 1100.0, 950.0).parallel());
}

void simpleReduce(Stream<String> stringStream) {
  String reduceResult = stringStream.reduce(
      "",
      (a, b) -> {
        IO.println("Acc: %s + %s = %s (Thread %s)".formatted(a, b, a + b, Thread.currentThread().getName()));
        return a + b;
      });
  IO.println(reduceResult);
}

void reduceWithCombiner(Stream<Character> characterStream) {
  String reduceResult = characterStream.reduce(
      "",
      (a, b) -> {
        IO.println("Acc: %s + %s = %s (Thread %s)".formatted(a, b, a + b, Thread.currentThread().getName()));
        return a + b;
      },
      (a, b) -> {
        IO.println("Comb: %s + %s = %s (Thread %s)".formatted(a, b, a + b, Thread.currentThread().getName()));
        return a + b;
      });
  IO.println(reduceResult);
}

void fold(Stream<Character> stringStream) {
  Optional<String> foldResult = stringStream.gather(Gatherers.fold(() -> "",
      (String a, Character b) -> {
        IO.println("%s + %s = %s (Thread %s)".formatted(a, b, a + b, Thread.currentThread().getName()));
        return a + b;
      })
  ).findFirst();
  foldResult.ifPresentOrElse(
      res ->  IO.println(res),
      () -> IO.println("fold result not present")
  );
}

void weightedAverageReduce(Stream<Double> sensorValues) {
  double initialValue = 1000.0;
  double weightedAverageSensorValue = sensorValues.reduce(
      initialValue,
      (currentWeightedAverage, newValue) -> {
        IO.println("(%f * 0.9) + (%f * 0.1) = %f".formatted(currentWeightedAverage, newValue, (currentWeightedAverage * 0.9) + (newValue * 0.1)));
        return (currentWeightedAverage * 0.9) + (newValue * 0.1);
      }
  );
  IO.println("Weighted average: " + weightedAverageSensorValue + " ppm");
}

void weightedAverageFold(Stream<Double> sensorValues) {
  double initialValue = 1000.0;
  double weightedAverageSensorValue = sensorValues
      .gather(Gatherers.fold(
          () -> initialValue,
          (currentWeightedAverage, newValue) -> {
            IO.println("(%f * 0.9) + (%f * 0.1) = %f".formatted(currentWeightedAverage, newValue, (currentWeightedAverage * 0.9) + (newValue * 0.1)));
            return (currentWeightedAverage * 0.9) + (newValue * 0.1);
          }
      )).findFirst().orElseThrow();
  IO.println("Weighted average: " + weightedAverageSensorValue + " ppm");
}