namespace java net.ndolgov.thriftrpctest.api

struct TestRequest {
    1: required i64 requestId
}

struct TestResponse {
    1: required bool success
    2: required i64 requestId
    3: string result
}

service TestService {
    TestResponse process(TestRequest request)
}