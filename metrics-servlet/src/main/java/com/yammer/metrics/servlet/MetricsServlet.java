package com.yammer.metrics.servlet;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An HTTP servlet which outputs the metrics in a {@link MetricsRegistry} (and optionally the data
 * provided by {@link VirtualMachineMetrics}) in a JSON object. Only responds to {@code GET}
 * requests.
 * <p/>
 * If the servlet context has an attribute named
 * {@code com.yammer.metrics.servlet.MetricsServlet.registry} which is a
 * {@link MetricsRegistry} instance, {@link MetricsServlet} will use it instead of {@link Metrics}.
 * <p/>
 * {@link MetricsServlet} also takes an initialization parameter, {@code show-jvm-metrics}, which
 * should be a boolean value (e.g., {@code "true"} or {@code "false"}). It determines whether or not
 * JVM-level metrics will be included in the JSON output.
 * <p/>
 * {@code GET} requests to {@link MetricsServlet} can make use of the following query-string
 * parameters:
 * <dl>
 *     <dt><code>/metrics?class=com.example.service</code></dt>
 *     <dd>
 *         <code>class</code> is a string used to filter the metrics in the JSON by metric name. In
 *         the given example, only metrics for classes whose canonical name starts with
 *         <code>com.example.service</code> would be shown. You can also use <code>jvm</code> for
 *         just the JVM-level metrics.
 *     </dd>
 *
 *     <dt><code>/metrics?pretty=true</code></dt>
 *     <dd>
 *         <code>pretty</code> determines whether or not the JSON which is returned is printed with
 *         indented whitespace or not. If you're looking at the JSON in the browser, use this.
 *     </dd>
 *
 *     <dt><code>/metrics?full-samples=true</code></dt>
 *     <dd>
 *         <code>full-samples</code> determines whether or not the JSON which is returned will
 *         include the full content of histograms' and timers' reservoir samples. If you're
 *         aggregating across hosts, you may want to do this to allow for more accurate quantile
 *         calculations.
 *     </dd>
 * </dl>
 */
public class MetricsServlet extends HttpServlet {

    /**
     * The attribute name of the {@link MetricsRegistry} instance in the servlet context.
     */
    public static final String REGISTRY_ATTRIBUTE = MetricsServlet.class.getName() + ".registry";

    /**
     * The attribute name of the {@link JsonFactory} instance in the servlet context.
     */
    public static final String JSON_FACTORY_ATTRIBUTE = JsonFactory.class.getCanonicalName();

    /**
     * The initialization parameter name which determines whether or not JVM_level metrics will be
     * included in the JSON output.
     */
    public static final String SHOW_JVM_METRICS = "show-jvm-metrics";

    private static final JsonFactory DEFAULT_JSON_FACTORY = new JsonFactory(new ObjectMapper());
    private static final String CONTENT_TYPE = "application/json";

    private final Clock clock;
    private final VirtualMachineMetrics vm;
    private MetricsRegistry registry;
    private JsonFactory factory;
    private boolean showJvmMetrics;
    private MetricsJsonGenerator metricsJsonGenerator;
    private MetricsJsonGenerator.MetricsContext metricsContext;

    /**
     * Creates a new {@link MetricsServlet}.
     */
    public MetricsServlet() {
        this(Clock.defaultClock(), VirtualMachineMetrics.getInstance(),
             Metrics.defaultRegistry(), DEFAULT_JSON_FACTORY, true);
    }

    /**
     * Creates a new {@link MetricsServlet}.
     *
     * @param showJvmMetrics    whether or not JVM-level metrics will be included in the output
     */
    public MetricsServlet(boolean showJvmMetrics) {
        this(Clock.defaultClock(), VirtualMachineMetrics.getInstance(),
             Metrics.defaultRegistry(), DEFAULT_JSON_FACTORY, showJvmMetrics);
    }

    /**
     * Creates a new {@link MetricsServlet}.
     *
     * @param clock             the clock used for the current time
     * @param vm                a {@link VirtualMachineMetrics} instance
     * @param registry          a {@link MetricsRegistry}
     * @param factory           a {@link JsonFactory}
     * @param showJvmMetrics    whether or not JVM-level metrics will be included in the output
     */
    public MetricsServlet(Clock clock,
                          VirtualMachineMetrics vm,
                          MetricsRegistry registry,
                          JsonFactory factory,
                          boolean showJvmMetrics) {
        this.clock = clock;
        this.vm = vm;
        this.registry = registry;
        this.factory = factory;
        this.showJvmMetrics = showJvmMetrics;
        this.metricsJsonGenerator = new MetricsJsonGenerator();
        this.metricsContext = new MetricsJsonGenerator.MetricsContext(this.clock,this.vm,this.registry);
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        final Object factory = config.getServletContext()
                                     .getAttribute(JSON_FACTORY_ATTRIBUTE);
        if (factory instanceof JsonFactory) {
            this.factory = (JsonFactory) factory;
        }

        final Object o = config.getServletContext().getAttribute(REGISTRY_ATTRIBUTE);
        if (o instanceof MetricsRegistry) {
            this.registry = (MetricsRegistry) o;
            this.metricsContext = new MetricsJsonGenerator.MetricsContext(this.clock,this.vm,this.registry);
        }

        final String showJvmMetricsParam = config.getInitParameter(SHOW_JVM_METRICS);
        if (showJvmMetricsParam != null) {
            this.showJvmMetrics = Boolean.parseBoolean(showJvmMetricsParam);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String classPrefix = req.getParameter("class");
        final boolean pretty = Boolean.parseBoolean(req.getParameter("pretty"));
        final boolean showFullSamples = Boolean.parseBoolean(req.getParameter("full-samples"));

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(CONTENT_TYPE);
        final OutputStream output = resp.getOutputStream();
        final JsonGenerator json = factory.createJsonGenerator(output, JsonEncoding.UTF8);
        if (pretty) {
            json.useDefaultPrettyPrinter();
        }
        json.writeStartObject();
        {
            if (showJvmMetrics && ("jvm".equals(classPrefix) || classPrefix == null)) {
                metricsJsonGenerator.writeVmMetrics(json, this.metricsContext);
            }

            metricsJsonGenerator.writeRegularMetrics(json, classPrefix, showFullSamples, this.metricsContext);
        }
        json.writeEndObject();
        json.close();
    }
}
