package zingg.common.py.annotations;

import javax.annotation.processing.*;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;

@Target({ElementType.TYPE})
public @interface PythonClass {
	
	public String module();
	
}