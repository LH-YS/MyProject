package com.LY.udfs.udf;

import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class FlinkRateLimiterUDF extends ScalarFunction {

    private static final Logger log = LoggerFactory.getLogger(FlinkRateLimiterUDF.class);
    private static final Long DEFAULT_VALUE = Long.valueOf(1000L);
    private String rateLimitNum;
    private RateLimiter rateLimiter;


    public void open(FunctionContext context) throws IOException {
        log.info("FlinkRateLimiterUDF init");
        rateLimitNum = context.getJobParameter("rate_limit_num","1000");
        log.info("FlinkRateLimiterUDF open method finished");
    }

    public String eval(String what_ever) {

        String rate = rateLimitNum;
        if (StringUtils.isBlank(rate)) {
            return what_ever;
        }
        if (StringUtils.isNotBlank(rate)) {
            Long rateR = Long.valueOf(rate);
            if (this.rateLimiter == null) {
                this.rateLimiter = RateLimiter.create(rateR.longValue());
            } else if (rateR.longValue() != this.rateLimiter.getRate()) {
                this.rateLimiter.setRate(rateR.longValue());
            }
        }
        this.rateLimiter.acquire(1);
        return what_ever;
    }
}