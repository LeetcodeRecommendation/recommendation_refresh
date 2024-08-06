package com.leetcoders.job_scheduling.streams;

import java.util.List;

public record UserRecommendationRefreshRequest(String name, String token, String csrfToken, List<String> companies) {
}
