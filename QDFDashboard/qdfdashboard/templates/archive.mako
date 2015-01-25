<!doctype html>
<html class="no-js" lang="en">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Distillate Dashboard</title>
        <link rel="stylesheet" href="static/css/foundation.css" />
        <script src="static/js/vendor/modernizr.js"></script>
        <link href='http://fonts.googleapis.com/css?family=Dosis|News+Cycle:400,700' rel='stylesheet' type='text/css'>
    </head>

    <body style="background-color:#d3f2fc">
        <div style="width:100%">
            <div id="header">
                <div class="top-panel">
                    <div style="background-color:#55B7D7; text-align:center; padding-top:2rem; padding-bottom:2rem;">
                        <div class="top-panel-text" align="center">
                            <h1><font color="white">Distillate Processing Dashboard</font></h1>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div id="container">
            <!-- Content Part -->
            <div class="row" style="padding-top:2em">
                <font face="Dosis">
                    <ul class="accordion" data-accordion>
                        %for instance in instances:
                            <li class="accordion-navigation">
                                <a href="#inst-${instance["name"]}">${instance["name"]}</a>
                                <div id="inst-${instance["name"]}" class="content">
                                    <ul class="accordion" data-accordion>
                                        %for year in instance["rows"]:
                                            %for month in year["rows"]: #skip year
                                                <li class="accordion-navigation">
                                                    <% icon="/static/img/good.png" if month["ok"] else "/static/img/bad.png" %>
                                                    <a href="#inst-${instance["name"]}-mon-${month["name"]}"><img src="${icon}"> ${year["name"]} Month ${month["name"]}</a>
                                                    <div id="inst-${instance["name"]}-mon-${month["name"]}" class="content">
                                                        <ul class="accordion" data-accordion>
                                                            %for day in month["rows"]:
                                                                <li class="accordion-navigation">
                                                                    <%
                                                                        target=request.route_url("daylist", inst=instance["name"], year=year["name"], month=month["name"], day=day["name"])
                                                                        theid="inst-%s-mon-%s-d-%s" % (instance["name"], month["name"], day["name"])
                                                                        #
                                                                    %>
                                                                    <% icon="/static/img/good.png" if day["ok"] else "/static/img/bad.png" %>
                                                                    <a href="#${theid}" onclick="$('#${theid}').load('${target}')"><img src="${icon}"> Day ${day["name"]}</a>
                                                                    <div id="${theid}" class="content">
                                                                        <p>Loading from server</p>
                                                                    </div>
                                                                </li>
                                                            %endfor
                                                        </ul>
                                                    </div>
                                                </li>
                                            %endfor
                                        %endfor
                                    </ul>
                                </div>
                            </li>
                        %endfor
                    </ul>
                </font>
            </div>
            <!--Footer-->
            <div class="row">
                <div id="footer">
                    <div class="bott-panel">
                        <div style="background-color:#D3f2fc; padding-top:1rem">
                            <div class="bott-panel-text">
                                <div class="large-11 columns">
                                    <p style="color:#000000; font-size:13px">This site is managed by Michael P Andersen</p>
                                </div>
                           </div>
                        </div>
                    </div>
                </div>
            </div>
            <!--End Footer-->
        </div>

    <script src="static/js/vendor/jquery.js"></script>
    <script src="static/js/foundation.min.js"></script>
    <script>
        $(document).foundation();
    </script>

    </body>
</html>