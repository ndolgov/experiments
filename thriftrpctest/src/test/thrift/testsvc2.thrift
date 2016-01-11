namespace java net.ndolgov.thriftrpctest.api

struct TestRequest2 {
    1: required i64 requestId
}

struct TestResponse2 {
    1: required bool success
    2: required i64 requestId
    3: string result
}

service TestService2 {
    TestResponse2 process(TestRequest2 request)
}