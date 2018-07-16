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
    record.add(name: "some_int", bin: AsBin(100500));
    record.add(name: "some_double", bin: AsBin(100.5001));
    record.add(name: "some_string", bin: AsBin("test3"));
    record.add(name: "some_boolean", bin: AsBin(false));

    conn.set(set: "test", key: "100500", record: record);
//  conn.set(namespase: "ns", set: "test", key: "100500", record: record);
```



## get

```swift
//  if let val = conn.get(namespase: "ns", set: "test", key: "100500") {
    if let val = conn.get(set: "test", key: "100500") {
        print("some_int", val["some_int"]!.integer!)
        print("some_double", val["some_double"]!.double!)
        print("some_string", val["some_string"]!.string)
        print("some_boolean", val["some_boolean"]!.boolean)
    }
```