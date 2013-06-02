package com.yammer.metrics.servlet;

import com.fasterxml.jackson.core.JsonGenerator;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.MetricDispatcher;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;


/**
 * Writes the json structure returned by the servlet.  This class can be used to generate json outside of servlet;
 * meaning anything that provides the MetricsJsonGenerator.MetricsContext and JsonGenerator, no dependence on servlet.
 */
public class MetricsJsonGenerator implements MetricProcessor<MetricsJsonGenerator.JsonContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsJsonGenerator.class);

    static final class JsonContext {
        final boolean showFullSamples;
        final JsonGenerator json;

        JsonContext(JsonGenerator json, boolean showFullSamples) {
            this.json = json;
            this.showFullSamples = showFullSamples;
        }
    }

    public static final class MetricsContext {
        private final Clock clock;
        private final VirtualMachineMetrics vm;
        private final MetricsRegistry registry;

        public MetricsContext(Clock clock, VirtualMachineMetrics vm, MetricsRegistry registry) {
            this.clock = clock;
            this.vm = vm;
            this.registry = registry;
        }

        public Clock getClock() {
            return clock;
        }

        public VirtualMachineMetrics getVm() {
            return vm;
        }

        public MetricsRegistry getRegistry() {
            return registry;
        }
    }

    public void writeVmMetrics(JsonGenerator json, MetricsContext metricsContext) throws IOException {
        VirtualMachineMetrics vm = metricsContext.getVm();
        json.writeFieldName("jvm");
        json.writeStartObject();
        {
            json.writeFieldName("vm");
            json.writeStartObject();
            {
                json.writeStringField("name", vm.getName());
                json.writeStringField("version", vm.getVersion());
            }
            json.writeEndObject();

            json.writeFieldName("memory");
            json.writeStartObject();
            {
                json.writeNumberField("totalInit", vm.getTotalInit());
                json.writeNumberField("totalUsed", vm.getTotalUsed());
                json.writeNumberField("totalMax", vm.getTotalMax());
                json.writeNumberField("totalCommitted", vm.getTotalCommitted());

                json.writeNumberField("heapInit", vm.getHeapInit());
                json.writeNumberField("heapUsed", vm.getHeapUsed());
                json.writeNumberField("heapMax", vm.getHeapMax());
                json.writeNumberField("heapCommitted", vm.getHeapCommitted());

                json.writeNumberField("heap_usage", vm.getHeapUsage());
                json.writeNumberField("non_heap_usage", vm.getNonHeapUsage());
                json.writeFieldName("memory_pool_usages");
                json.writeStartObject();
                {
                    for (Map.Entry<String, Double> pool : vm.getMemoryPoolUsage().entrySet()) {
                        json.writeNumberField(pool.getKey(), pool.getValue());
                    }
                }
                json.writeEndObject();
            }
            json.writeEndObject();

            final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats = vm.getBufferPoolStats();
            if (!bufferPoolStats.isEmpty()) {
                json.writeFieldName("buffers");
                json.writeStartObject();
                {
                    json.writeFieldName("direct");
                    json.writeStartObject();
                    {
                        json.writeNumberField("count", bufferPoolStats.get("direct").getCount());
                        json.writeNumberField("memoryUsed", bufferPoolStats.get("direct").getMemoryUsed());
                        json.writeNumberField("totalCapacity", bufferPoolStats.get("direct").getTotalCapacity());
                    }
                    json.writeEndObject();

                    json.writeFieldName("mapped");
                    json.writeStartObject();
                    {
                        json.writeNumberField("count", bufferPoolStats.get("mapped").getCount());
                        json.writeNumberField("memoryUsed", bufferPoolStats.get("mapped").getMemoryUsed());
                        json.writeNumberField("totalCapacity", bufferPoolStats.get("mapped").getTotalCapacity());
                    }
                    json.writeEndObject();
                }
                json.writeEndObject();
            }


            json.writeNumberField("daemon_thread_count", vm.getDaemonThreadCount());
            json.writeNumberField("thread_count", vm.getThreadCount());
            json.writeNumberField("current_time", metricsContext.getClock().getTime());
            json.writeNumberField("uptime", vm.getUptime());
            json.writeNumberField("fd_usage", vm.getFileDescriptorUsage());

            json.writeFieldName("thread-states");
            json.writeStartObject();
            {
                for (Map.Entry<Thread.State, Double> entry : vm.getThreadStatePercentages()
                    .entrySet()) {
                    json.writeNumberField(entry.getKey().toString().toLowerCase(),
                        entry.getValue());
                }
            }
            json.writeEndObject();

            json.writeFieldName("garbage-collectors");
            json.writeStartObject();
            {
                for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.getGarbageCollectors()
                    .entrySet()) {
                    json.writeFieldName(entry.getKey());
                    json.writeStartObject();
                    {
                        final VirtualMachineMetrics.GarbageCollectorStats gc = entry.getValue();
                        json.writeNumberField("runs", gc.getRuns());
                        json.writeNumberField("time", gc.getTime(TimeUnit.MILLISECONDS));
                    }
                    json.writeEndObject();
                }
            }
            json.writeEndObject();
        }
        json.writeEndObject();
    }

    public void writeRegularMetrics(
        JsonGenerator json,
        String classPrefix,
        boolean showFullSamples,
        MetricsContext metricsContext) throws IOException {

        final MetricDispatcher dispatcher = new MetricDispatcher();
        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry :
            metricsContext.getRegistry().getGroupedMetrics().entrySet()) {
            if (classPrefix == null || entry.getKey().startsWith(classPrefix)) {
                json.writeFieldName(entry.getKey());
                json.writeStartObject();
                {
                    for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                        json.writeFieldName(subEntry.getKey().getName());
                        try {
                            dispatcher.dispatch(subEntry.getValue(), subEntry.getKey(), this, new JsonContext(json, showFullSamples));
                        } catch (Exception e) {
                            LOGGER.warn("Error writing out " + subEntry.getKey(), e);
                        }
                    }
                }
                json.writeEndObject();
            }
        }
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, JsonContext context) throws Exception {
        final JsonGenerator json = context.json;
        json.writeStartObject();
        {
            json.writeStringField("type", "histogram");
            json.writeNumberField("count", histogram.getCount());
            writeSummarizable(histogram, json);
            writeSampling(histogram, json);

            if (context.showFullSamples) {
                json.writeObjectField("values", histogram.getSnapshot().getValues());
            }
        }
        json.writeEndObject();
    }

    @Override
    public void processCounter(MetricName name, Counter counter, JsonContext context) throws Exception {
        final JsonGenerator json = context.json;
        json.writeStartObject();
        {
            json.writeStringField("type", "counter");
            json.writeNumberField("count", counter.getCount());
        }
        json.writeEndObject();
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, JsonContext context) throws Exception {
        final JsonGenerator json = context.json;
        json.writeStartObject();
        {
            json.writeStringField("type", "gauge");
            json.writeObjectField("value", evaluateGauge(gauge));
        }
        json.writeEndObject();
    }

    @Override
    public void processMeter(MetricName name, Metered meter, JsonContext context) throws Exception {
        final JsonGenerator json = context.json;
        json.writeStartObject();
        {
            json.writeStringField("type", "meter");
            json.writeStringField("event_type", meter.getEventType());
            writeMeteredFields(meter, json);
        }
        json.writeEndObject();
    }

    @Override
    public void processTimer(MetricName name, Timer timer, JsonContext context) throws Exception {
        final JsonGenerator json = context.json;
        json.writeStartObject();
        {
            json.writeStringField("type", "timer");
            json.writeFieldName("duration");
            json.writeStartObject();
            {
                json.writeStringField("unit", timer.getDurationUnit().toString().toLowerCase());
                writeSummarizable(timer, json);
                writeSampling(timer, json);
                if (context.showFullSamples) {
                    json.writeObjectField("values", timer.getSnapshot().getValues());
                }
            }
            json.writeEndObject();

            json.writeFieldName("rate");
            json.writeStartObject();
            {
                writeMeteredFields(timer, json);
            }
            json.writeEndObject();
        }
        json.writeEndObject();
    }

    private static Object evaluateGauge(Gauge<?> gauge) {
        try {
            return gauge.getValue();
        } catch (RuntimeException e) {
            LOGGER.warn("Error evaluating gauge", e);
            return "error reading gauge: " + e.getMessage();
        }
    }

    private static void writeSummarizable(Summarizable metric, JsonGenerator json) throws IOException {
        json.writeNumberField("min", metric.getMin());
        json.writeNumberField("max", metric.getMax());
        json.writeNumberField("mean", metric.getMean());
        json.writeNumberField("std_dev", metric.getStdDev());
    }

    private static void writeSampling(Sampling metric, JsonGenerator json) throws IOException {
        final Snapshot snapshot = metric.getSnapshot();
        json.writeNumberField("median", snapshot.getMedian());
        json.writeNumberField("p75", snapshot.get75thPercentile());
        json.writeNumberField("p95", snapshot.get95thPercentile());
        json.writeNumberField("p98", snapshot.get98thPercentile());
        json.writeNumberField("p99", snapshot.get99thPercentile());
        json.writeNumberField("p999", snapshot.get999thPercentile());
    }

    private static void writeMeteredFields(Metered metered, JsonGenerator json) throws IOException {
        json.writeStringField("unit", metered.getRateUnit().toString().toLowerCase());
        json.writeNumberField("count", metered.getCount());
        json.writeNumberField("mean", metered.getMeanRate());
        json.writeNumberField("m1", metered.getOneMinuteRate());
        json.writeNumberField("m5", metered.getFiveMinuteRate());
        json.writeNumberField("m15", metered.getFifteenMinuteRate());
    }
}
