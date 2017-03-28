var colls = db.getCollectionNames();
for (var i = 0; i < colls.length; i++) {
  var name = colls[i];   
  if(name.substr(0, 6) != 'system')
    print(name + ' - ' + db[name].count());
	
}

