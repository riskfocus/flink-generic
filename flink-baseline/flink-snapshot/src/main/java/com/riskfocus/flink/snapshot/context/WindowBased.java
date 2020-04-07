package com.riskfocus.flink.snapshot.context;

import com.riskfocus.flink.domain.TimeAware;
import com.riskfocus.flink.util.DateTimeUtils;
import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.WindowContext;
import lombok.AllArgsConstructor;

/**
 * Implementation based on Window approach
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class WindowBased implements ContextService {

    private final WindowAware windowAware;

    @Override
    public ContextMetadata generate(TimeAware timeAware, String contextName) {
        WindowContext windowContext = windowAware.generateWindowPeriod(timeAware.getTimestamp());
        String dateStr = DateTimeUtils.formatDate(windowContext.getStart());
        return ContextMetadata.builder().id(windowContext.getId()).date(dateStr).contextName(contextName).build();
    }

    @Override
    public void init() {

    }

    @Override
    public void close() {

    }
}
