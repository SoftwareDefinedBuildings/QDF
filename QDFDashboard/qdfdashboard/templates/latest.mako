<%inherit file="basecontent.mako"/>

<%block name="content">
    <table>
        <tr>
            <th>Run name</th>
            <th>Instance</th>
            <th>Time elapsed</th>
            <th>Exit status</th>
            <th>Log file</th>
        <tr>
        % for row in runs:
            <tr>
                <td>${row["run_id"] | h}</td>
                <td>${row["instance"] | h}</td>
                <td>${"%.2fs" % (row["time"]) | h}</td>
                <td>${row["retcode_human"] | h}</td>
                <td><a href="${request.route_url("logfile",id=row["logname"])}">${row["logname"]}</a></td>
            </tr>
        % endfor
    </table>
</%block>