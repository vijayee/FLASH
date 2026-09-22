/// Web stub for the Task 7 native status-file seam: browsers have no
/// process environment or appendable files, so both calls are no-ops and
/// the web example keeps its URL-param-only configuration.
String? envOverride(String name) => null;

void appendStatusLine(String path, String json) {}