package org.cloudcmp.servlets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Service;
import org.cloudcmp.Task;
import org.json.simple.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

public class APIServlet extends HttpServlet {
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setContentType("text/x-json");
		if (req.getParameter("op") == null) {
			resp.sendError(500);
		}
		else if (req.getParameter("op").equals("listadaptors")) {
			JSONValue.writeJSONString(Adaptor.listAdaptors(), resp.getWriter());			
		}
		else {
			String adaptorName = req.getParameter("adaptor");
			if (adaptorName == null) {
				resp.sendError(500);
				return;
			}
			Adaptor adaptor = Adaptor.getAdaptorByName(adaptorName);
			if (adaptor == null) {
				resp.sendError(404);
				return;
			}		
			if (req.getParameter("op").equals("listtasks")) {
				JSONValue.writeJSONString(Task.listTasks(adaptor), resp.getWriter());
			}
			else if (req.getParameter("op").equals("listservices")) {
				JSONValue.writeJSONString(Service.listServices(adaptor), resp.getWriter());
			}
			else if (req.getParameter("op").equals("listadaptorconfigs")) {
				Map<String, String> defaultConfigs = new HashMap<String, String>();
				for (String item : adaptor.getConfigItems()) {
					defaultConfigs.put(item, adaptor.configs.get(item));
				}				
				JSONValue.writeJSONString(defaultConfigs, resp.getWriter());
			}
			else if (req.getParameter("op").equals("listtaskconfigs")) {
				String taskName = req.getParameter("task");
				if (taskName == null) {
					resp.sendError(500);
					return;
				}
				Task task = Task.getTaskByName(taskName, adaptor);
				if (task == null) {
					resp.sendError(404);
					return;
				}
				Map<String, String> defaultConfigs = new HashMap<String, String>();
				for (String item : task.getConfigItems()) {
					defaultConfigs.put(item, task.configs.get(item));
				}
				JSONValue.writeJSONString(defaultConfigs, resp.getWriter());
			}
			else if (req.getParameter("op").equals("startservice")) {
				Map<String, String []> paramMap = req.getParameterMap();
				for (String key: paramMap.keySet()) {
					if (key.startsWith("ac_")) {
						String configKey = key.substring(3);
						String configValue = (paramMap.get(key))[0];
						adaptor.configs.put(configKey, configValue);
					}
				}
				String serviceName = req.getParameter("service");
				if (serviceName == null) {
					resp.sendError(500);
					return;
				}
				Service service = Service.getServiceByName(serviceName, adaptor);
				if (service == null) {
					resp.sendError(404);
					return;
				}
				Thread thread = new Thread(service);
				Service.runningServices.add(service);
				Service.runningServiceThreads.add(thread);
				thread.start();
			}
			else if (req.getParameter("op").equals("runtask")) {
				Map<String, String []> paramMap = req.getParameterMap();
				for (String key: paramMap.keySet()) {
					if (key.startsWith("ac_")) {
						String configKey = key.substring(3);
						String configValue = (paramMap.get(key))[0];
						adaptor.configs.put(configKey, configValue);
					}
				}
				String taskName = req.getParameter("task");
				if (taskName == null) {
					resp.sendError(500);
					return;
				}
				Task task = Task.getTaskByName(taskName, adaptor);
				if (task == null) {
					resp.sendError(404);
					return;
				}
				for (String key: paramMap.keySet()) {
					if (key.startsWith("tc_")) {
						String configKey = key.substring(3);
						String configValue = (paramMap.get(key))[0];
						task.configs.put(configKey, configValue);
					}
				}
				JSONValue.writeJSONString(task.run(), resp.getWriter());				
			}
			else if (req.getParameter("op").equals("starttask")) {
				Map<String, String []> paramMap = req.getParameterMap();
				for (String key: paramMap.keySet()) {
					if (key.startsWith("ac_")) {
						String configKey = key.substring(3);
						String configValue = (paramMap.get(key))[0];
						adaptor.configs.put(configKey, configValue);
					}
				}
				String taskName = req.getParameter("task");
				if (taskName == null) {
					resp.sendError(500);
					return;
				}
				Task task = Task.getTaskByName(taskName, adaptor);
				if (task == null) {
					resp.sendError(404);
					return;
				}
				for (String key: paramMap.keySet()) {
					if (key.startsWith("tc_")) {
						String configKey = key.substring(3);
						String configValue = (paramMap.get(key))[0];
						task.configs.put(configKey, configValue);
					}
				}
				Map<String, String> res = new HashMap<String, String>();
				if (!task.isAsyncSupported()) {
					res.put("exception", "Async is not supported for this task");
				}
				else {
					long handle = task.start();				
					res.put("handle", String.valueOf(handle));
				}
				JSONValue.writeJSONString(res, resp.getWriter());	
			}
			else if (req.getParameter("op").equals("stoptask")) {
				String taskName = req.getParameter("task");
				if (taskName == null) {
					resp.sendError(500);
					return;
				}
				Task task = Task.getTaskByName(taskName, adaptor);
				if (task == null) {
					resp.sendError(404);
					return;
				}
				if (req.getParameter("handle") == null) {
					resp.sendError(500);
					return;					
				}
				long handle = Long.parseLong(req.getParameter("handle"));
				Map<String, String> res = new HashMap<String, String>();
				if (!task.isAsyncSupported()) {
					res.put("exception", "Async is not supported for this task");
				}
				else {
					task.stop(handle);				
					res.put("status", "ok");
				}
				JSONValue.writeJSONString(res, resp.getWriter());				
			}
			else if (req.getParameter("op").equals("taskisfinished")) {
				String taskName = req.getParameter("task");
				if (taskName == null) {
					resp.sendError(500);
					return;
				}
				Task task = Task.getTaskByName(taskName, adaptor);
				if (task == null) {
					resp.sendError(404);
					return;
				}
				if (req.getParameter("handle") == null) {
					resp.sendError(500);
					return;					
				}
				long handle = Long.parseLong(req.getParameter("handle"));
				Map<String, String> res = new HashMap<String, String>();
				if (!task.isAsyncSupported()) {
					res.put("exception", "Async is not supported for this task");
				}
				else {
					boolean finished = task.isFinished(handle);					
					res.put("finished", String.valueOf(finished));
				}
				JSONValue.writeJSONString(res, resp.getWriter());
			}
			else if (req.getParameter("op").equals("taskretrieveresults")) {
				String taskName = req.getParameter("task");
				if (taskName == null) {
					resp.sendError(500);
					return;
				}
				Task task = Task.getTaskByName(taskName, adaptor);
				if (task == null) {
					resp.sendError(404);
					return;
				}
				if (req.getParameter("handle") == null) {
					resp.sendError(500);
					return;					
				}
				long handle = Long.parseLong(req.getParameter("handle"));
				Map<String, String> res = new HashMap<String, String>();
				if (!task.isAsyncSupported()) {
					res.put("exception", "Async is not supported for this task");
				}
				else {										
					res = task.retrieveResults(handle);
				}
				JSONValue.writeJSONString(res, resp.getWriter());
			}
			else {
				resp.sendError(404);
			}
		}
	}
}
