$.ajax({
  type: "GET",
  url: ("https://www.googleapis.com/storage/v1/b/hail-common/o/annotationdb%2f" +
      hail_version +
      "%2fdatasets.json?alt=media"),
  dataType: "json",
  success: function(data) {
    for (let name in data) {
      let dataset = data[name];
      let versions_string = dataset.versions.map(function(i) {
        let version = i["version"];
        if (!version) version = "None";
        return version;
      }).reduce(function(i, j) {
        return i + "<br>" + j;
      });
      let ref_genome_string = dataset.versions.map(function(i) {
        let rg = i["reference_genome"];
        if (!rg) rg = "None";
        return rg;
      }).reduce(function(i, j) {
        return i + "<br>" + j;
      });
      let tr = $("<tr/>");
      tr.append("<td>" + name + "</td>");
      tr.append("<td>" + dataset.description + "\n<a href='" + dataset.url +
          "'>link</a></td>");
      tr.append("<td>" + versions_string + "</td>");
      tr.append("<td>" + ref_genome_string + "</td>");
      $(".table1").append(tr);
    }
  },
});

function filterTable() {
  let input = document.getElementById("searchInput");
  let filter = input.value.toUpperCase();
  let table = document.getElementById("table1");
  let tr = table.getElementsByTagName("tr");
  let found = false;
  for (let i = 0; i < tr.length; i++) {
    let td = tr[i].getElementsByTagName("td");
    for (let j = 0; j < td.length; j++) {
      if (td[j].innerHTML.toUpperCase().indexOf(filter) > -1) {
        found = true;
      }
    }
    if (found) {
      tr[i].style.display = "";
      found = false;
    } else if (!tr[i].id.match("^tableHeader")) {
      tr[i].style.display = "none";
    }
  }
}
