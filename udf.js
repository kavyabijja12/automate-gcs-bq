function transform(inJson) {
    val = inJson.split(",");
  
    const obj = { 
        "Name": val[0], 
        "Rollno": parseInt(val[1]),
        "Age": parseInt(val[2]),
        "Gender": val[3],
        "College": val[4]
    };
    return JSON.stringify(obj);
  }