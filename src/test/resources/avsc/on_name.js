function onNameUpdated() {
    var age = record.get("age");
    notify(age);
    notify(http_get);
    http_get.apply("http://localhost:8080/ping");
    http_post.apply("http://localhost:8080/personinfo/put/2/age", "888");
    for (i = 0; i < fields.length; i++) {
        var field = fields[i];
        notify(field._1);
        notify(field._2);
    }
}

function notify(value) {
    print(id + ":" + value);
}

onNameUpdated();
