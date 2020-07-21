package kafkaadmin.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Map;

public class RoleBinding {
    public String principal;
    public String role;
    public Map<String, Map<String,String>> scope;
    public ArrayList<RoleBindingResource> resources;
    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RoleBinding rhs = (RoleBinding) obj;
        return new EqualsBuilder()
                .append(principal, rhs.principal)
                .append(role, rhs.role)
                .append(scope, rhs.scope)
                .append(resources, rhs.resources)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).
                append(principal).
                append(role).
                append(scope).
                append(resources).
                toHashCode();
    }
    public RoleBinding()
    {
        principal = null;
        role = null;
        scope = null;
        resources = null;
    }
    public RoleBinding(RoleBinding copyRoleBinding)
    {
        this.principal = new String(copyRoleBinding.principal);
        this.role = new String(copyRoleBinding.role);
        // ugly but we do not need a full copy
        this.scope = copyRoleBinding.scope;
        this.resources = copyRoleBinding.resources;
    }
}

