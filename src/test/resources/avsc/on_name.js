    function onNameUpdated() {
        var age = record.get("age");
        what_is(age);

        what_is(http_get);
        var http_get_result = http_get.apply("http://localhost:8080/ping", 5);
        java.lang.Thread.sleep(1000);
        what_is(http_get_result.value());

        what_is(http_post);
        var http_post_result = http_post.apply("http://localhost:8080/personinfo/put/2/age", "888", 5);
        java.lang.Thread.sleep(1000);
        what_is(http_post_result.value());

        for (i = 0; i < binlogs.length; i++) {
            var binlog = binlogs[i];
            what_is(binlog.type());
            what_is(binlog.xpath());
            what_is(binlog.value());
        }
    }

    function what_is(value) {
        print(id + ": " + value);
    }

    onNameUpdated();
