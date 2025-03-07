package usjpin.flink.examples;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class InfiniteSource implements SourceFunction<Integer> {
  private final Random random;
  private final int maxValue;
  private boolean isRunning = true;
  
  public InfiniteSource(int maxValue) {
    this.maxValue = maxValue;
    this.random = new Random();
  }
  
  @Override
  public void run(SourceContext<Integer> ctx) throws Exception {
    while (isRunning) {
      ctx.collect(random.nextInt(maxValue));
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}
