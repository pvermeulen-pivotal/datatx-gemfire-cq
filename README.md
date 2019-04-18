# GemFire Server CQs Close Function
## GemFire V8 Only
The project provides a function to stop and close all active client continuous queries on cache server(s). 

### GemFire
<function-service>
	<function>
		<class-name>datatx.util.gemfire.cq.CloseServerCqs</class-name>
	</function>
</function-service>

### GemFire Client
The following property must be set on the Cient Connection Pool to ensure client receives event that server continuous queries have been stopped and closed.

setPoolSubscriptionTimeoutMultiplier(1)

### Spring
<gfe:function-service>
	<gfe:function>
		<bean class="datatx.util.gemfire.cq.CloseServerCqs" />
	</gfe:function>
</gfe:functio-service>

### GFSH Command

#### Deploy Function
deploy --jar=datatx-gemfire-cq-1.0.0-RELEASE.jar

#### Function Execution
execute function --id=CloseServerCqs --members="server1 server2"

#### Results: List of CQ queries closed

