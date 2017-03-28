var mx_resources = ["MXCreditMajor", "MXNamePersonPrimaryImage", "MXNormalizedVideoWorkGrouping", "MXNormalizedVideoWorkRelease", "MXSourcePrimaryLogos", "MXVideoWorkGroupingPrimaryImage", "MXVideoWorkPrimaryImage"];

var pks = {"MXCreditMajor": "CreditId", "MXNamePersonPrimaryImage": "NamePersonId", "MXNormalizedVideoWorkGrouping": "MXNormalizedVideoWorkGroupingId", "MXNormalizedVideoWorkRelease": "MXNormalizedVideoWorkGroupingId", "MXSourcePrimaryLogos": "SourceId", "MXVideoWorkGroupingPrimaryImage": "VideoWorkGroupingId", "MXVideoWorkPrimaryImage": "VideoWorkId"};

mx_resources.forEach(function(coll) {
  var pk = pks[coll];
  var query = {};
  query[pk] = {$ne: null};
  print(coll + ' - ' + db[coll].count(query));
});
