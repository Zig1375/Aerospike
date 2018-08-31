# Aerospike Swift client


## Connection

```swift
//  if let conn = Connection(host: "127.0.0.1") {
    if let conn = Connection(host: "127.0.0.1", namespace: "ns") {
        // Your code here
    }
```



## put

```swift
    let record = AsRecord();
    record.add(name: "some_int", bin: 100500);
    record.add(name: "some_double", bin: 100.5001);
    record.add(name: "some_string", bin: "Hello Wolrd");
    record.add(name: "some_list", bin: List(array: [1,2,3]));
    record.add(name: "some_map", bin: Map(map: [1:"1", 2: "2"]));

    conn.set(set: "test", key: "100500", record: record);
//  conn.set(namespase: "ns", set: "test", key: "100500", record: record);
```



## get

```swift
//  if let val = conn.get(namespase: "ns", set: "test", key: "100500") {
    if let val = conn.get(set: "test", key: "100500") {
        let someInt  = val["some_int"];
        let someDbl  = val["some_double"];
        let someBool = val["some_string"];
        let someList = val["some_list"];
        let someMap  = val["some_map"];
    }
```