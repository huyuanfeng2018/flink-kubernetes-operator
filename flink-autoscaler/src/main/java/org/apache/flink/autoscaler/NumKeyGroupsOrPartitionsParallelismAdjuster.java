package org.apache.flink.autoscaler;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import static org.apache.flink.autoscaler.JobVertexScaler.SCALE_LIMITED_MESSAGE_FORMAT;
import static org.apache.flink.autoscaler.JobVertexScaler.SCALING_LIMITED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Component responsible adjusts the parallelism of a vertex that knows the number of partitions or
 * a vertex whose upstream shuffle is key by.
 */
public class NumKeyGroupsOrPartitionsParallelismAdjuster {

    public static <KEY, Context extends JobAutoScalerContext<KEY>> int adjust(
            JobVertexID vertex,
            Context context,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            int maxParallelism,
            int numSourcePartitions,
            int newParallelism,
            int upperBound,
            int parallelismLowerLimit) {

        var numKeyGroupsOrPartitions =
                numSourcePartitions <= 0 ? maxParallelism : numSourcePartitions;

        Mode mode =
                context.getConfiguration()
                        .get(AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE);

        var upperBoundForAlignment =
                Math.min(
                        // Optimize the case where newParallelism <= maxParallelism / 2
                        newParallelism > numKeyGroupsOrPartitions / 2
                                ? numKeyGroupsOrPartitions
                                : numKeyGroupsOrPartitions / 2 + numKeyGroupsOrPartitions % 2,
                        upperBound);

        // When the shuffle type of vertex inputs contains keyBy or vertex is a source,
        // we try to adjust the parallelism such that it divides
        // the numKeyGroupsOrPartitions without a remainder => data is evenly spread across subtasks
        for (int p = newParallelism; p <= upperBoundForAlignment; p++) {
            if (numKeyGroupsOrPartitions % p == 0
                    ||
                    // When MAXIMIZE_UTILISATION is enabled, Try to find the smallest parallelism
                    // that
                    // can satisfy the current consumption rate.
                    (mode == Mode.MAXIMIZE_UTILISATION
                            && numKeyGroupsOrPartitions / p
                                    < numKeyGroupsOrPartitions / newParallelism)) {
                return p;
            }
        }

        // When adjusting the parallelism after rounding up cannot
        // find the right degree of parallelism to meet requirements,
        // Try to find the smallest parallelism that can satisfy the current consumption rate.
        int p =
                calculateMinimumParallelism(
                        numKeyGroupsOrPartitions, newParallelism, parallelismLowerLimit);
        var message =
                String.format(
                        SCALE_LIMITED_MESSAGE_FORMAT,
                        vertex,
                        newParallelism,
                        p,
                        numKeyGroupsOrPartitions,
                        upperBound,
                        parallelismLowerLimit);
        eventHandler.handleEvent(
                context,
                AutoScalerEventHandler.Type.Warning,
                SCALING_LIMITED,
                message,
                SCALING_LIMITED + vertex + newParallelism,
                context.getConfiguration().get(SCALING_EVENT_INTERVAL));
        return p;
    }

    private static int calculateMinimumParallelism(
            int numKeyGroupsOrPartitions, int newParallelism, int parallelismLowerLimit) {
        int p = newParallelism;
        for (; p > 0; p--) {
            if (numKeyGroupsOrPartitions / p > numKeyGroupsOrPartitions / newParallelism) {
                if (numKeyGroupsOrPartitions % p != 0) {
                    p++;
                }
                break;
            }
        }
        p = Math.max(p, parallelismLowerLimit);
        return p;
    }

    /** The mode of the parallelism adjustment. */
    public enum Mode implements DescribedEnum {
        DEFAULT(
                "This mode ensures that the parallelism adjustment attempts to evenly distribute data across subtasks"
                        + ". It is particularly effective for source vertices that are aware of partition counts or vertices after "
                        + "'keyBy' operation. The goal is to have the number of key groups or partitions be divisible by the set parallelism, ensuring even data distribution and reducing data skew."),

        MAXIMIZE_UTILISATION(
                "This model is to maximize resource utilization. In this mode, an attempt is made to set"
                        + " the minimum degree of parallelism that meets the current consumption rate requirements. Unlike the default mode, it is not enforced that the number of key groups or partitions is divisible by the degree of parallelism."),
        ;

        private final InlineElement description;

        Mode(String description) {
            this.description = text(description);
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
