package com.cognizant.notificationService.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

import java.util.List;

@Configuration
public class CorsConfig {

    /**
     * Enable CORS for browser-based clients (Vite dev server defaults to 5173).
     *
     * Note: If you're using cookies/session auth, keep allowCredentials=true and
     * use explicit origins (no "*").
     */
    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration();
        // If you rely on cookies, keep allowCredentials=true and avoid wildcards.
        // For our current setup (JWT in headers / no cookies), disabling credentials
        // simplifies local testing (Postman, gateway, Vite dev server).
        config.setAllowCredentials(false);

        // Allow common local dev origins. Using patterns also tolerates slight
        // host variations and avoids brittle exact-origin lists.
        config.setAllowedOriginPatterns(List.of(
                "http://localhost:*",
                "http://127.0.0.1:*",
                // Local API clients may run as a browser extension (e.g., Apidog) and
                // always send an Origin like chrome-extension://<id>.
                "chrome-extension://*",
                // Some tools use a custom app scheme.
                "app://*"));
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
        config.addAllowedHeader("*");
        config.addExposedHeader("Location");
        config.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return new CorsFilter(source);
    }
}
