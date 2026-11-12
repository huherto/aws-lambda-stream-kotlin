package org.myorg.urls;

import io.kopipes.aws.AwsEnvironmentConfig;

/** The environment config for the url bff: the usual env vars, with the dev bus when BUS_NAME is not set. */
final class UrlsConfig {
    private UrlsConfig() {
    }

    static AwsEnvironmentConfig envConfig() {
        return new AwsEnvironmentConfig() {
            @Override
            public String busName() {
                String busName = super.busName();
                return busName != null ? busName : "urls-dev-bus";
            }
        };
    }
}
