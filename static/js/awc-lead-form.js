(function () {
  "use strict";

  function readFormPayload(form) {
    var data = {};
    new FormData(form).forEach(function (value, key) {
      data[key] = String(value).trim();
    });
    return data;
  }

  function bindAwcLeadForm(form) {
    if (!form || form.dataset.bound === "true") return;
    form.dataset.bound = "true";

    var successId = form.getAttribute("data-success-id") || "awc-lead-form-success";
    var source = form.getAttribute("data-source") || "homepage";
    var successEl = document.getElementById(successId);

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!form.checkValidity()) {
        form.reportValidity();
        return;
      }

      var payload = readFormPayload(form);
      var email = payload.email || "";

      if (email && typeof DataPulse !== "undefined" && typeof DataPulse.identifyUser === "function") {
        DataPulse.identifyUser(email, {
          email: email,
          first_name: payload.firstName || undefined,
          last_name: payload.lastName || undefined,
          company: payload.companyName || undefined,
          job_title: payload.jobTitle || undefined,
          country: payload.country || undefined,
          source: "awc_lead_form",
        });
      }

      if (typeof DataPulse !== "undefined" && typeof DataPulse.trackEvent === "function") {
        DataPulse.trackEvent("awc_lead_form_submitted", {
          form: "schedule_architecture_deep_dive",
          source: source,
          first_name: payload.firstName,
          last_name: payload.lastName,
          email: email,
          company: payload.companyName,
          job_title: payload.jobTitle,
          country: payload.country,
          has_phone: Boolean(payload.businessPhone),
        });
      }

      if (successEl) {
        successEl.classList.remove("hidden");
      }
      form.reset();
      var countrySelect = form.querySelector('[name="country"]');
      if (countrySelect) {
        countrySelect.value = "us";
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll(".lead-form").forEach(bindAwcLeadForm);
  });

  window.DataPulseLeadForm = {
    bind: bindAwcLeadForm,
  };
})();
