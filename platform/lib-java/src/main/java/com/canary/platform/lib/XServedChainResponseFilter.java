package com.canary.platform.lib;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XServedChainResponseFilter implements Filter, Ordered {

    private final String ownToken;

    public XServedChainResponseFilter(String serviceName, String version) {
        String svc = (serviceName == null || serviceName.isBlank()) ? "unknown" : serviceName.trim();
        String ver = (version == null || version.isBlank()) ? "stable" : version.trim();
        this.ownToken = svc + "=" + ver;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 300;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        XServedChainContext.clear();
        try {
            chain.doFilter(request, response);
            if (response instanceof HttpServletResponse http) {
                List<String> all = new ArrayList<>();
                all.add(ownToken);
                all.addAll(XServedChainContext.tokens());
                http.setHeader(XServedChainContext.HEADER_NAME, String.join(",", all));
            }
        } finally {
            XServedChainContext.clear();
        }
    }
}
