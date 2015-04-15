    function onNameUpdated() {
        var age = record.get("age");
        what_is(age);

        what_is(http_get);
        var http_get_result = http_get.apply("http://localhost:8080/ping");
        java.lang.Thread.sleep(1000);
        what_is(http_get_result.value());

        what_is(http_post);
        var http_post_result = http_post.apply("http://localhost:8080/personinfo/put/2/age", "888");
        java.lang.Thread.sleep(1000);
        what_is(http_post_result.value());

        for (i = 0; i < fields.length; i++) {
            var field = fields[i];
            what_is(field._1);
            what_is(field._2);
        }
    }

    function what_is(value) {
        print(id + ": " + value);
    }

    onNameUpdated();
    notify_finished.apply();